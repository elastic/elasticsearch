/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * The ES|QL query language.
 *
 * <h2>Overview</h2>
 * ES|QL is a typed query language which consists of many small languages separated by the {@code |}
 * character. Like this:
 *
 * <pre>{@code
 *   FROM foo
 * | WHERE a > 1
 * | STATS m=MAX(j)
 * | SORT m ASC
 * | LIMIT 10
 * }</pre>
 *
 * <p>
 *    Here the {@code FROM}, {@code WHERE}, {@code STATS}, {@code SORT}, and {@code LIMIT} keywords
 *    enable the mini-language for selecting indices, filtering documents, calculate aggregates,
 *    sorting results, and limiting the number of results respectively.
 * </p>
 *
 * <h2>Language Design Goals</h2>
 * In designing ES|QL we have some principals and rules of thumb:
 * <ul>
 *     <li>Don't waste people's time</li>
 *     <li>Progress over perfection</li>
 *     <li>Design for Elasticsearch</li>
 *     <li>Be inspired by the best</li>
 * </ul>
 *
 * <h3>Don't waste people's time</h3>
 * <ul>
 *     <li>Queries should not fail at runtime. Instead we should return a
 *         {@link org.elasticsearch.compute.operator.Warnings warning} and {@code null}.</li>
 *     <li>It <strong>is</strong> ok to fail a query up front at analysis time. Just not after it's
 *         started.</li>
 *     <li>It <strong>is better</strong> if things can be made to work.</li>
 *     <li>But genuinely confusing requests require the query writing to make a choice.</li>
 * </ul>
 * <p>
 *     As you can see this is a real tight rope, but we try to follow the rules above in order. Examples:
 * </p>
 * <ul>
 *     <li>If {@link org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime TO_DATETIME}
 *         receives an invalid date at runtime, it emits a WARNING.</li>
 *     <li>If {@link org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract DATE_EXTRACT}
 *         receives an invalid extract configuration at query parsing time it fails to start the query.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add 1 + 3.2}
 *         promotes both sides to a {@code double}.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add 1 + "32"}
 *         fails at query compile time and the query writer must decide to either write
 *         {@code CONCAT(TO_STRING(1), "32")} or {@code 1 + TO_INT("32")}.</li>
 * </ul>
 *
 * <h3>Progress over perfection</h3>
 * <ul>
 *     <li>Stability is super important for released features.</li>
 *     <li>But we need to experiment and get feedback. So mark features {@code experimental} when
 *         there's any question about how they should work.</li>
 *     <li>Experimental features shouldn't live forever because folks will get tired of waiting
 *         and use them in production anyway. We don't officially support them in production but
 *         we will feel bad if they break.</li>
 * </ul>
 *
 * <h3>Design for Elasticsearch</h3>
 * We must design the language <strong>for</strong> Elasticsearch, celebrating its advantages
 * smoothing out its and quirks.
 * <ul>
 *     <li>{@link org.elasticsearch.index.fielddata doc_values} sometimes sorts field values and
 *         sometimes sorts and removes duplicates. We couldn't hide this even if we want to and
 *         most folks are ok with it. ES|QL has to be useful in those cases.</li>
 *     <li>Multivalued fields are very easy to index in Elasticsearch so they should be easy to
 *         read in ES|QL. They <strong>should</strong> be easy to work with in ES|QL too, but we
 *         haven't gotten that far yet.</li>
 * </ul>
 *
 * <h3>Be inspired by the best</h3>
 * We'll frequently have lots of different choices on how to implement a feature. We should talk
 * and figure out the best way for us, especially considering Elasticsearch's advantages and quirks.
 * But we should also look to our data-access-forebears:
 * <ul>
 *     <li><a href="https://www.postgresql.org/docs/current/functions.html">PostgreSQL</a> is the
 *         <a href="https://www.wikidata.org/wiki/Q120920482">GOAT</a> SQL implementation. It's a joy
 *         to use for everything but dates. Use <a href="https://www.db-fiddle.com/f/xgMswzqBb6GYQ5uzwcYzi8/0">DB Fiddle</a>
 *         to link to syntax examples.</li>
 *     <li><a href="https://docs.oracle.com/cd/E17952_01/mysql-5.7-en/date-and-time-functions.html">Oracle</a>
 *         is pretty good about dates. It's fine about a lot of things but PostgreSQL is better.</li>
 *     <li>MS <a href="https://learn.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver16">SQL Server</a>
 *         has a silly name but its documentation is <strong>wonderful</strong>.</li>
 *     <li><a href="https://docs.splunk.com/Documentation/SCS/current/SearchReference/UnderstandingSPLSyntax">SPL</a>
 *         is super familiar to our users, and is a piped query language.</li>
 * </ul>
 *
 * <h2>Major Components</h2>
 *
 * <h3>Compute Engine</h3>
 *{@link org.elasticsearch.compute} - The compute engine drives query execution
 * <ul>
 *     <li>{@link org.elasticsearch.compute.data.Block} - fundamental unit of data.  Operations vectorize over blocks.</li>
 *     <li>{@link org.elasticsearch.compute.data.Page} - Data is broken up into pages (which are collections of blocks) to
 *     manage size in memory</li>
 * </ul>
 *
 * <h3>Core Classes</h3>
 * {@link org.elasticsearch.xpack.esql.core} - Core Classes
 * <ul>
 *     <li>{@link org.elasticsearch.xpack.esql.session.EsqlSession} - Connects all major components and contains the high-level code for
 *     query execution</li>
 *     <li>{@link org.elasticsearch.xpack.esql.core.type.DataType} - ES|QL is a typed language, and all the supported data types
 *     are listed in this collection.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.core.expression.Expression} - Expression is the basis for all functions in ES|QL,
 *     but see also {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper}</li>
 *     <li>{@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry} - Resolves function names to
 *         function implementations.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.action.RestEsqlQueryAction Sync} and
 *         {@link org.elasticsearch.xpack.esql.action.RestEsqlAsyncQueryAction async} HTTP API entry points</li>

 * </ul>
 *
 * <h3>Query Planner</h3>
 * <p>The query planner encompasses the logic of how to serve a query. Essentially, this covers everything from the output of the Antlr
 * parser through to the actual computations and lucene operations.</p>
 * <p>Two key concepts in the planner layer:</p>
 * <ul>
 *     <li>Logical vs Physical optimization - Logical optimizations refer to things that can be done strictly based on the structure
 *     of the query, while Physical optimizations take into account information about the index or indices the query will execute
 *     against</li>
 *     <li>Local vs non-local operations - "local" refers to operations happening on the data nodes, while non-local operations generally
 *     happen on the coordinating node and can apply to all participating nodes in the query</li>
 * </ul>
 * <h4>Query Planner Steps</h4>
 * <ul>
 *     <li>{@link org.elasticsearch.xpack.esql.parser.LogicalPlanBuilder LogicalPlanBuilder} translates from Antlr data structures to our
 *     data structures</li>
 *      <li>{@link org.elasticsearch.xpack.esql.analysis.PreAnalyzer PreAnalyzer} finds involved indices</li>
 *     <li>{@link org.elasticsearch.xpack.esql.analysis.Analyzer Analyzer} resolves references</li>
 *     <li>{@link org.elasticsearch.xpack.esql.analysis.Verifier Verifier} does type checking</li>
 *     <li>{@link org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer LogicalPlanOptimizer} applies many optimizations</li>
 *     <li>{@link org.elasticsearch.xpack.esql.planner.mapper.Mapper Mapper} translates logical plans to phyisical plans</li>
 *     <li>{@link org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer PhysicalPlanOptimizer} - decides what plan fragments to
 *     send to which data nodes</li>
 *     <li>{@link org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer LocalLogicalPlanOptimizer} applies index-specific
 *     optimizations, and reapplies top level logical optimizations</li>
 *     <li>{@link org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer LocalPhysicalPlanOptimizer} Lucene push down and
 *     similar</li>
 *     <li>{@link org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner LocalExecutionPlanner} Creates the compute engine objects
 *     to carry out the query</li>
 * </ul>
 *
 *
 * <h2>Guides</h2>
 * <ul>
 * <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar Writing scalar functions}</li>
 * <li>{@link org.elasticsearch.xpack.esql.expression.function.aggregate Writing aggregation functions}</li>
 * </ul>
 *
 * <h2>Code generation</h2>
 * ES|QL uses two kinds of code generation which is uses mostly to
 * <a href="https://en.wikipedia.org/wiki/Monomorphization">monomorphize</a> tight loops. That process would
 * require a lot of copy-and-paste with small tweaks and some of us have copy-and-paste blindness so instead
 * we use code generation.
 * <ol>
 *     <li>When possible we use <a href="https://www.stringtemplate.org/">StringTemplate</a> to build
 *         Java files. These files typically look like {@code X-Blah.java.st} and are typically used for things
 *         like the different {@link org.elasticsearch.compute.data.Block} types and their subclasses and
 *         aggregation state. The templates themselves are easy to read and edit. This process is appropriate
 *         for cases where you just have to copy and paste something and change a few lines here and there. See
 *         {@code build.gradle} for the code generators.</li>
 *     <li>When that doesn't work, we use
 *         <a href="https://docs.oracle.com/en/java/javase/17/docs/api/java.compiler/javax/annotation/processing/package-summary.html">
 *         Annotation processing</a> and <a href="https://github.com/square/javapoet">JavaPoet</a> to build the Java files.
 *         These files are typically the inner loops for {@link org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator}
 *         or {@link org.elasticsearch.compute.aggregation.AggregatorFunction}. The code generation is much more difficult
 *         to write and debug but much, much, much, much more flexible. The degree of control we have during this
 *         code generation is amazing but it is much harder to debug failures. See files in
 *         {@code org.elasticsearch.compute.gen} for the code generators.</li>
 * </ol>
 */

package org.elasticsearch.xpack.esql;
