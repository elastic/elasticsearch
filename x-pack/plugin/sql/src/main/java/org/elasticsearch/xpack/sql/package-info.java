/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * <b>X-Pack SQL</b> module is a SQL interface to Elasticsearch. <br>
 * In a nutshell, currently, SQL acts as a <i>translator</i>, allowing
 * traditional SQL queries to be executed against Elasticsearch indices
 * without any modifications. Do note that SQL does <i>not</i> try to hide
 * Elasticsearch or abstract it in anyway; rather it maps the given SQL,
 * if possible, to one (at the moment) query DSL. Of course, this means
 * not all SQL queries are supported.<br>
 *
 * <h2>Premise</h2>
 * Since Elasticsearch is not a database nor does it supports arbitrary
 * {@code JOIN}s (a cornerstone of SQL), SQL module is built from the
 * ground-up with Elasticsearch in mind first and SQL second. In fact,
 * even the grammar introduces Elasticsearch specific components that
 * have no concept in ANSI SQL.
 *
 * <h2>Architecture</h2>
 * SQL module is roughly based on the Volcano project (by Graefe
 * {@code &} co)
 * <a href="http://ieeexplore.ieee.org/document/344061">[1]</a>
 * <a href="https://dl.acm.org/citation.cfm?id=627558">[2]</a>
 * <a href="https://scholar.google.com/citations?user=pdDeRScAAAAJ">[3]</a>
 *
 * which argues for several design principles, from which 2 are relevant
 * to this project, namely:
 *
 * <dl>
 *  <dt>Logical and Physical algebra</dt>
 *  <dd>Use of extensible algebraic and logical set of operators to
 *  describe the operation to underlying engine. The engine job is to
 *  map a user query into logical algebra and then translate this into
 *  physical algebra.</dd>
 *  <dt>Rules to identify patterns</dt>
 *  <dd>The use of <i>rules</i> as a way to identify relevant
 *  <i>pattern</i>s inside the plans that can be worked upon</dd>
 * </dl>
 *
 * In other words, the use of a logical plan, which represents what the
 * user has requested and a physical plan which is what the engine needs
 * to execute based on the user request. To manipulate the plans, the
 * engine does pattern matching implemented as rules that get applied over
 * and over until none matches.
 * An example of a rule would be expanding {@code *} to actual concrete
 * references.
 *
 * As a side-note, the Volcano model has proved quite popular being used
 * (to different degrees) by the majority of SQL engines out there such
 * as Apache Calcite, Apache Impala, Apache Spark and Facebook Presto.
 *
 * <h2>Concepts</h2>
 *
 * The building operation of the SQL engine is defined by an action,
 * namely a rule (defined in {@link org.elasticsearch.xpack.sql.rule rule}
 * package that accepts one <i>immutable</i> tree (defined in
 * {@link org.elasticsearch.xpack.sql.tree tree} package) and transforms
 * it to another <i>immutable</i> tree.
 * Each rules looks for a certain <i>pattern</i> that it can identify and
 * then transform.
 *
 * The engine works with 3 main type of trees:
 *
 * <dl>
 *   <dt>Logical plan</dt>
 *   <dd><i>Logical</i> representation of a user query. Any transformation
 *   of this plan should result in an <i>equivalent</i> plan - meaning for
 *   the same input, it will generate the same output.</dd>
 *   <dt>Physical plan</dt>
 *   <dd><i>Execution</i> representation of a user query. This plan needs
 *   to translate to (currently) one query to Elasticsearch. It is likely
 *   in the future (once we look into supporting {@code JOIN}s, different
 *   strategies for generating a physical plan will be available depending
 *   on the cost. </dd>
 *   <dt>Expression tree</dt>
 *   <dd>Both the logical and physical plan contain an expression trees
 *   that need to be incorporated into the query. For the most part, most
 *   of the work inside the engine resolves around expressions.</dd>
 * </dl>
 *
 * All types of tree inside the engine have the following properties:
 * <dl>
 *   <dt>Immutability</dt>
 *   <dd>Each node and its properties are immutable. A change in a property
 *   results in a new node which results in a new tree.</dd>
 *   <dt>Resolution</dt>
 *   <dd>Due to the algebraic nature of SQL, each tree has the notion of
 *   resolution which indicates whether it has been resolved or not. A node
 *   can be resolved only if it <b>and</b> its children have all been
 *   resolved.</dd>
 *   <dt>Traversal</dt>
 *   <dd>Each tree can be traversed top-to-bottom/pre-order/parents-first or
 *   bottom-up/post-order/children-first. The difference in the traversal
 *   depends on the pattern that is being identified.</dd>
 * </dl>
 *
 * A typical flow inside the engine is the following:
 *
 * <ol>
 *  <li>The engine is given a query</li>
 *  <li>The query is parsed and transformed into an <i>unresolved</i> AST or
 *  logical plan</li>
 *  <li>The logical plan gets analyzed and resolved</li>
 *  <li>The logical plan gets optimized</li>
 *  <li>The logical plan gets transformed into a physical plan</li>
 *  <li>The physical plan gets mapped and then folded into an Elasticsearch
 *  query</li>
 *  <li>The Elasticsearch query gets executed</li>
 * </ol>
 *
 * <h3>Digression - Visitors, pattern matching, {@code instanceof} and
 * Java 10/11/12</h3>
 *
 * To implement the above concepts, several choices have been made in the
 * engine (which are not common in the rest of the XPack code base). In
 * particular the conventions/signatures of
 * {@link org.elasticsearch.xpack.sql.tree.Node tree}s and usage of
 * {@code instanceof} inside
 * {@link org.elasticsearch.xpack.sql.rule.Rule rule}s).
 * Java doesn't provide any utilities for tree abstractions or pattern
 * matching for that matter. Typically for tree traversal one would employ
 * the <a href="https://en.wikipedia.org/wiki/Visitor_pattern">Visitor</a>
 * pattern however that is not a suitable candidate for SQL because:
 * <ul>
 * <li>the visitor granularity is a node and patterns are likely to involve
 * multiple nodes</li>
 * <li>transforming a tree and identifying a pattern requires holding a
 * state which means either the tree or the visitor become stateful</li>
 * <li>a node can stop traversal (which is not desired)</li>
 * <li>it's unwieldy - every node type requires a dedicated {@code visit}
 * method</li>
 * </ul>
 *
 * While in Java, there might be hope for
 * <a href="http://cr.openjdk.java.net/~briangoetz/amber/pattern-match.html">the future</a>
 * Scala has made it a
 * <a href="https://docs.scala-lang.org/tour/pattern-matching.html">core feature</a>.
 * Its byte-code implementation is less pretty as it relies on
 * {@code instanceof} checks. Which is how many rules are implemented in
 * the SQL engine as well. Where possible though, one can use <i>typed</i>
 * traversal by passing a {@code Class} token to the lambdas (i.e.
 * {@link org.elasticsearch.xpack.sql.tree.Node#transformDown(java.util.function.Function, Class)
 * pre-order transformation}).
 *
 * <h2>Components</h2>
 *
 * The SQL engine is made up of the following components:
 * <dl>
 *  <dt>{@link org.elasticsearch.xpack.sql.parser Parser} package</dt>
 *  <dd>Tokenizer and Lexer of the SQL grammar. Translates user query into an
 *  AST tree ({@code LogicalPlan}. Makes sure the user query is <b>syntactically</b>
 *  valid.</dd>
 *  <dt>{@link org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer PreAnalyzer}</dt>
 *  <dd>Performs basic inspection of the {@code LogicalPlan} for gathering critical
 *  information for the main analysis. This stage is separate from {@code Analysis}
 *  since it performs async/remote calls to the cluster. </dd>
 *  <dt>{@link org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer Analyzer}</dt>
 *  <dd>Performs {@code LogicalPlan} analysis, resolution and verification. Makes
 *  sure the user query is actually valid and <b>semantically</b> valid.</dd>
 *  <dt>{@link org.elasticsearch.xpack.sql.optimizer.Optimizer Optimizer}</dt>
 *  <dd>Transforms the <i>resolved</i> {@code LogicalPlan} into a <i>semantically</i>
 *  equivalent tree, meaning for the same input, the same output is produced.</dd>
 *  <dt>{@link org.elasticsearch.xpack.sql.planner.Planner Planner}</dt>
 *  <dd>Performs query planning. The planning is made up of two components:
 *  <dl>
 *     <dt>{@code Mapper}</dt>
 *     <dd>Maps the {@code LogicalPlan} to a {@code PhysicalPlan}</dd>
 *     <dt>{@code Folder}</dt>
 *     <dd>Folds or rolls-up the {@code PhysicalPlan} into an Elasticsearch
 *     {@link org.elasticsearch.xpack.sql.plan.physical.EsQueryExec executable query}
 *     </dd>
 *  </dl>
 *  </dd>
 *  <dt>{@link org.elasticsearch.xpack.sql.execution Execution}</dt>
 *  <dd>Actual execution of the query, results retrieval, extractions and translation
 *  into a {@link org.elasticsearch.xpack.sql.session.RowSet tabular} format.</dd>
 * </dl>
 */
package org.elasticsearch.xpack.sql;
