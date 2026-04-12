<h1>🚀 Databricks CDC Star Schema Engine</h1>

<p>
A <strong>config-driven data pipeline engine</strong> built on Databricks and Apache Spark for generating 
<strong>incremental fact tables</strong> using <strong>CDC (Change Data Capture)</strong> and dynamically joining 
dimension tables to produce a <strong>star schema</strong>.
</p>

<hr>

<h2>📌 Overview</h2>

<p>This project implements a reusable framework to:</p>

<ul>
    <li>Build <strong>incremental fact tables</strong> from source data</li>
    <li>Dynamically join multiple <strong>dimension tables</strong></li>
    <li>Support <strong>CDC-based processing</strong> using timestamp columns</li>
    <li>Generate queries programmatically using configuration</li>
    <li>Enable scalable <strong>medallion architecture (Bronze → Silver → Gold)</strong></li>
</ul>

<p>
The engine abstracts business logic into configuration, allowing flexible and reusable pipeline design across datasets.
</p>

<hr>

<h2>⚙️ Key Features</h2>

<ul>
    <li>
        <strong>CDC-Based Incremental Loads</strong><br>
        Processes only new or updated records using a configurable timestamp column.
    </li>
    <br>
    <li>
        <strong>Dynamic Star Schema Generation</strong><br>
        Automatically builds joins between fact and dimension tables using metadata-driven configuration.
    </li>
    <br>
    <li>
        <strong>Config-Driven Architecture</strong><br>
        All joins, columns, and mappings are defined in configuration — no hardcoding.
    </li>
    <br>
    <li>
        <strong>Delta Lake Integration</strong><br>
        Designed for seamless use with Delta tables, supporting scalable data warehousing patterns.
    </li>
    <br>
    <li>
        <strong>Reusable Query Engine</strong><br>
        SQL queries are generated dynamically, making the pipeline adaptable to multiple use cases.
    </li>
</ul>

<hr>

<h2>🧠 How It Works</h2>

<ol>
    <li>
        <strong>Determine Last Load Timestamp</strong>
        <ul>
            <li>Fetches the latest processed timestamp from the target table</li>
            <li>Falls back to a default value for initial loads</li>
        </ul>
    </li>
    <br>
    <li>
        <strong>Generate Dynamic SQL</strong>
        <ul>
            <li>Selects required fact columns</li>
            <li>Joins dimension tables based on configured keys</li>
            <li>Adds surrogate keys from dimension tables</li>
            <li>Applies incremental filtering using CDC column</li>
        </ul>
    </li>
    <br>
    <li>
        <strong>Execute Transformation</strong>
        <ul>
            <li>Runs the generated SQL using Spark</li>
            <li>Produces a ready-to-load fact dataset</li>
        </ul>
    </li>
</ol>

<hr>

<h2>🔄 Incremental Processing Logic</h2>

<p>The engine ensures efficient processing by filtering only new data:</p>

<pre>
<code>
fact.modifiedDate &gt; last_processed_timestamp
</code>
</pre>

<p>This avoids:</p>

<ul>
    <li>Reprocessing historical data</li>
    <li>Duplicate loads</li>
    <li>Performance bottlenecks</li>
</ul>

<hr>

<h2>🧩 Configuration-Driven Design</h2>

<p>All transformations are controlled via configuration, including:</p>

<ul>
    <li>Fact table source</li>
    <li>Dimension tables and join keys</li>
    <li>Surrogate keys</li>
    <li>Columns to select</li>
</ul>

<p>This allows:</p>

<ul>
    <li>Rapid onboarding of new pipelines</li>
    <li>Minimal code changes</li>
    <li>High reusability</li>
</ul>
