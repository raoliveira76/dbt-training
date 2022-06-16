{% macro limit_data_in_dev(column_name) %}

    {% if target.name == 'development' %}
    WHERE {{ column_name }} >= dataadd('day', -30, current_timestamp)
    {% endif %}

{% endmacro %}