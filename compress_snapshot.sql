{% macro compress_snapshot(cte, id_col, subset_cols) %}
/*
This macro takes a snapshot which tracks changes to all source columns,
and compresses its timestamps based on a subset of columns that we would like to retain in staging.
*/

-- Subset of columns to include in subsequent queries
{% set subset_cols_str = subset_cols | join(', ') %}

-- If there is just 1 column, we don't concatenate, only cast to string.
{% if subset_cols | length == 1 %}
    {% set change_detection_cols =
        "coalesce(cast(" ~ subset_cols[0] ~ " as " ~ dbt.type_string() ~ "), '')"
    %}
-- Otherwise, we concatenate all change detection columns in order to use 1 single lag function.
{% else %}
    {% set lag_cols = [] %}
    {% for col in subset_cols %}
        {% do lag_cols.append(
            "coalesce(cast(" ~ col ~ " as " ~ dbt.type_string() ~ "), '')"
        ) %}
        {%- if not loop.last %}
        {%- do lag_cols.append("'-'") -%}
        {%- endif -%}
    {% endfor %}
    {% set change_detection_cols = dbt.concat(lag_cols) %}
{% endif %}

/*
Now we compare all subset columns with their previous version by ID.
The sum function will increase with every change, creating groups of integers.
*/
change_detection as (
    select
        {{ id_col }},
        {{ subset_cols_str }},
        sum(
            case
                when
                    {{ change_detection_cols }}
                    <>
                    lag({{ change_detection_cols }} ) over (partition by {{ id_col }} order by row_valid_from)
                    then 1
                else 0
            end
        ) over (partition by {{id_col}} order by row_valid_from) as compression_group,
        {{ get_audit_columns() }}
    from {{ cte }}
),

-- Compressing the date columns, grouping by the row groups we created.
compressed_snapshot as (
    select
        {{ id_col }},
        {{ subset_cols_str }},
        min(row_valid_from) as row_valid_from,
        max(row_valid_to) as row_valid_to,
        max(row_scd_id) as row_scd_id,
        min(row_updated_at) as row_updated_at,
        min(incremental_timestamp) as incremental_timestamp,
        max(row_processed_at) as row_processed_at,
    from change_detection
    group by
        {{ id_col }},
        {{ subset_cols_str }},
        compression_group
),

--Adding a PK with the compressed row_updated_at
{% set pk_col_name = 'pk_' ~ id_col | replace('_id', '') %}
final as (
    select
        {{ dbt_utils.generate_surrogate_key([id_col,'row_updated_at']) }} as {{pk_col_name}}
        ,{{ id_col }}
        ,{{ subset_cols_str }}
        ,{{ get_audit_columns() }}
    from compressed_snapshot
)

select * from final
where {{ get_default_incremental_predicate() }}

{% endmacro %}
