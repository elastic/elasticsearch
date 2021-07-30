/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <h2>Aggregations</h2>
 * <p>Builds analytic information over all hits in a search request.  Aggregations
 * are essentially a tool for sumarizing data, and that summary is often used
 * to generate a visualization.</p>
 *
 * <h2>Types of aggregations</h2>
 * There are three main types of aggregations, each in their own sub package:
 * <ul>
 *     <li>Bucket aggregations - which group documents (e.g. a histogram)</li>
 *     <li>Metric aggregations - which compute a summary value from several
 *     documents (e.g. a sum)</li>
 *     <li>Pipeline aggregations - which run as a seperate step and compute
 *     values across buckets</li>
 * </ul>
 * Additionally there is a support sub package, which contains the type checking
 * and resolution logic, primarily.
 *
 * <h2>How Aggregations Work</h2>
 * <p>TODO: Info about search phases goes here</p>
 *
 * <p>Aggregations operate in general as Map Reduce jobs.  The coordinating node for
 * the query dispatches the aggregation to each data node.  The data nodes all
 * instantiate an {@link org.elasticsearch.search.aggregations.AggregationBuilder}
 * of the appropriate type, which in turn builds the
 * {@link org.elasticsearch.search.aggregations.Aggregator} for that node.  This
 * collects the data from that shard, via
 * {@link org.elasticsearch.search.aggregations.Aggregator#getLeafCollector(org.apache.lucene.index.LeafReaderContext)}
 * more or less.  These values are shipped back to the coordinating node, which
 * performs the reduction on them (partial reductions in place on the data nodes
 * are also possible).</p>
 *
 * <h3>Three modes of operation</h3>
 * <p>When it comes to actually collecting values, there are three ways aggregations
 * operate, in general.  Which one we choose depends on limitations in the query
 * and how the data was ingested (e.g. if it is searchable).</p>
 *
 * <p>The easiest to understand is the <strong>Compatible</strong> (i.e. usable in
 * all situations) mode, which can be thought of as iterating each query hit and
 * collecting a value from it.  This is the least performant way to evaluate
 * aggregations, requiring looking at every hit.</p>
 *
 * <p>The fastest way to run an aggregation is by <strong>looking at the index structures
 * directly.</strong>  For example, Lucene just stores the minimum and maximum values
 * of fields per segment, so a min aggregation matching all documents in a segment
 * can just look up its result.  Generally speaking, this mode can be engaged when
 * there are no queries or sub-aggregations, and is gated by
 * {@link org.elasticsearch.search.aggregations.support.ValuesSourceConfig#getPointReaderOrNull()}.</p>
 *
 * <p>Finally, we can <strong>rewrite</strong> an aggregation into faster aggregations,
 * or ideally into just a query.  Generally, the goal here is to get to
 * <strong>filter by filters</strong> (which is an optimization on the filters aggregation
 * which runs it as a set of filter queries).  Often this process will look like rewriting
 * a DateHistogram into a DateRange, and then rewriting the DateRange into Filters.
 * If you see {@link org.elasticsearch.search.aggregations.AdaptingAggregator}, that's
 * a good clue that the rewrite mode is being used.  In general, when we rewrite aggregations,
 * we are able to detect if the rewritten agg can run in a "fast" mode, and decline the
 * rewrite if it can't.</p>
 *
 * <p>In general, aggs will try to use one of the fast modes, and if that's not possible,
 * fall back to running in compatible mode.</p>
 */
package org.elasticsearch.search.aggregations;
