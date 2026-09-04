/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Glob pattern matching and expansion for ES|QL external datasources.
 *
 * <p>This package handles path resolution for external data sources (S3, GCS, Azure, HTTP,
 * local filesystem) by expanding user-supplied patterns into concrete object paths.
 *
 * <h2>Supported Patterns</h2>
 *
 * <table border="1">
 * <caption>Glob pattern syntax reference</caption>
 * <tr>
 *   <th>Pattern</th>
 *   <th>Description</th>
 *   <th>Example</th>
 *   <th>Matches</th>
 * </tr>
 * <tr>
 *   <td>{@code *}</td>
 *   <td>Matches zero or more characters within a single path segment (does not cross {@code /}).</td>
 *   <td>{@code data/*.parquet}</td>
 *   <td>{@code data/sales.parquet}, {@code data/orders.parquet}</td>
 * </tr>
 * <tr>
 *   <td><code>**</code></td>
 *   <td>Matches zero or more path segments, including nested directories.</td>
 *   <td><code>data/&#42;&#42;/*.parquet</code></td>
 *   <td>{@code data/2024/01/file.parquet}, {@code data/file.parquet}</td>
 * </tr>
 * <tr>
 *   <td>{@code ?}</td>
 *   <td>Matches exactly one character (not {@code /}).</td>
 *   <td>{@code file-?.csv}</td>
 *   <td>{@code file-A.csv}, {@code file-1.csv}</td>
 * </tr>
 * <tr>
 *   <td>{@code [abc]}</td>
 *   <td>Matches one character from the set. Supports ranges ({@code [0-9]}) and
 *       negation ({@code [!abc]} or {@code [^abc]}).</td>
 *   <td>{@code file-[0-9].csv}</td>
 *   <td>{@code file-0.csv} through {@code file-9.csv}</td>
 * </tr>
 * <tr>
 *   <td>{@code {a,b,c}}</td>
 *   <td>Expands into one candidate per comma-separated alternative.
 *       Multiple groups produce the Cartesian product.</td>
 *   <td>{@code {sales,orders}/year={2024,2025}/*.parquet}</td>
 *   <td>{@code sales/year=2024/*.parquet}, {@code sales/year=2025/*.parquet},
 *       {@code orders/year=2024/*.parquet}, {@code orders/year=2025/*.parquet}</td>
 * </tr>
 * <tr>
 *   <td>{@code {N..M}}</td>
 *   <td>Numeric range expansion. Generates one candidate per integer in the range
 *       (inclusive, ascending or descending). Leading zeros on either operand enable
 *       zero-padded output using the wider operand's width (bash semantics).</td>
 *   <td>{@code shard-{000..099}.parquet}</td>
 *   <td>{@code shard-000.parquet}, {@code shard-001.parquet}, &hellip; {@code shard-099.parquet}</td>
 * </tr>
 * </table>
 *
 * <h2>Resolution Strategy</h2>
 *
 * <p>Patterns are resolved through one of two paths, chosen automatically:
 * <ul>
 *   <li><b>Brace-only fast path</b> &mdash; When the pattern contains only brace groups
 *       ({@code {a,b}} or {@code {N..M}}) and no other metacharacters, each expanded candidate
 *       is checked individually via {@code exists()} (HEAD request on cloud storage), avoiding
 *       a potentially expensive listing operation.</li>
 *   <li><b>Listing path</b> &mdash; When the pattern contains {@code *}, {@code ?}, or
 *       {@code [...]}, objects are listed under the longest non-pattern prefix and filtered
 *       through regex-based matching.</li>
 * </ul>
 *
 * <p>Both paths respect the {@code esql.external.max_glob_expansion} cluster setting (default 100)
 * which caps the number of expanded candidates from brace/range groups before falling back to listing,
 * and the {@code esql.external.max_discovered_files} setting which caps total discovered files.
 *
 * @see org.elasticsearch.xpack.esql.datasources.glob.GlobExpander
 * @see org.elasticsearch.xpack.esql.datasources.glob.BraceExpander
 * @see org.elasticsearch.xpack.esql.datasources.glob.GlobMatcher
 */
package org.elasticsearch.xpack.esql.datasources.glob;
