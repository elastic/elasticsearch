/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantOrderBy;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

/**
 * This is a marker interface for commands that maintain the sort order of their input. This commands can swap position with an adjacent
 * SORT ({@link OrderBy}) or with one that is separated from the command only by other SortPreserving commands.
 * <p>
 * These are some examples of commands that are <b>not</b> sort preserving:
 * <ul>
 *     <li>{@link Aggregate}: reducing node: maps input to different output. (Preceding SORTs should be dropped by the
 *          {@link PruneRedundantOrderBy} rule.)</li>
 *     <li>{@link Fork}: generative node: produces new data which might or might not be aligned with the existing sort.</li>
 *     <li>{@link Fuse}: reducing node: merges rows.</li>
 *     <li>{@link Join}: some types are generative; {@link LookupJoin} is surrogate'd with a "plain" Join node.</li>
 *     <li>{@link MvExpand}: generative node, can destabilize the order.</li>
 *     <li>{@link OrderBy}: introduces a new sort. </li>
 *     <li>{@link Limit}: truncating node. An outlier: it does maintain the order, but cannot be swapped with a SORT, as that'd change the
 *          produced results. Notably, the same applies to {@link Sample}, but that <i>is</i> acceptable within Sample's contract.</li>
 *     <li>{@link TopN}: introduces a new sort. </li>
 * </ul>
 * <p>
 * Most other commands are preserving the order.
 * <p>
 * Note: some commands, like Drop, Keep, Rename are also sort-preserving, but they get swapped out in the Analyzer, before their
 * sortability being relevant.
 */
public interface SortPreserving {}
