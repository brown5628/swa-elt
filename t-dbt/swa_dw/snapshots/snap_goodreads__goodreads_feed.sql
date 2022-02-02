{% snapshot snap_goodreads__goodreads_feed %}

{{
    config(
      target_database='small-world-analytics',
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols=['id', 'raw_data'],
    )
}}

select * from {{ source('raw_goodreads', 'goodreads_feed') }}

{% endsnapshot %}
