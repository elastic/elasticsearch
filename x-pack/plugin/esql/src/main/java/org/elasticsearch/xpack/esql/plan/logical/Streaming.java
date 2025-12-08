/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * This interface marks commands which do not add or remove rows and aren't sensitive to the exact order of the rows.
 * This is required to decide whether a command is compatible with
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.HoistRemoteEnrichLimit} and
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.HoistRemoteEnrichTopN}.
 * <p>
 * For the most part, such commands can be thought of to be operating on a row-by-row basis. A more formal definition is that this command
 * can be run on data nodes and this sequence
 *  <pre>{@code
 *  ... LIMIT X | MY_COMMAND
 *  }</pre>
 *  is safe to be replaced by this sequence
 *  <pre>{@code
 *  ... local LIMIT X | MY_COMMAND | LIMIT X
 *  }</pre>
 *  where "local" means that it's correct to apply the limit only on the data node, without a corresponding reduction on the coordinator.
 *  See {@link Limit#local()}.
 *  <p>
 *  We also require the same condition to hold for {@code TopN}, that is, the following are equivalent
 *  <pre>{@code
 *  ... TOP N [field1, ..., fieldN] | MY_COMMAND
 *  ... local TOP N [field1, ..., fieldN] | MY_COMMAND | TOP N [field1, ..., fieldN]
 *  }</pre>
 *  as long as MY_COMMAND preserves the columns that we order by.
 *  <p>
 *  Most commands that satisfy this will also satisfy the simpler (but stronger) conditions that the following are equivalent:
 *  <pre>{@code
 *  ... LIMIT X | MY_COMMAND
 *  ... MY_COMMAND | LIMIT X
 *
 *  and
 *
 * ... TOP N [field1, ..., fieldN] | MY_COMMAND
 * ... | MY_COMMAND | TOP N [field1, ..., fieldN]
 *  }</pre>
 *  <p>
 *  It is not true, for example, for WHERE:
 *  <pre>{@code
 *  ... TOP X [field] | WHERE side="dark"
 *  }</pre>
 *  If the first X rows do not contain any "dark" rows, the result is empty, however if we switch:
 *  <pre>{@code
 *  ... local TOP X [field] | WHERE side="dark" | TOP X [field]
 *  }</pre>
 *  and we have N nodes, then the first N*X rows may contain "dark" rows, and the final result is not empty in this case.
 *  <p>
 *      See also {@link PipelineBreaker}.
 */
public interface Streaming {}
