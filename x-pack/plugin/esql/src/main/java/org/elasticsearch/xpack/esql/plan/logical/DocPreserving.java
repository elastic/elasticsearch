/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

/**
 * Marker interface for commands where every output row still corresponds to exactly one document of the input. Adding columns to a row,
 * dropping whole rows, reordering rows, and fanning one row out into several that all describe the same document all preserve that
 * binding; deriving a row from more than one document does not.
 * <p>
 * This lets a command reason about a predicate written further upstream. {@code HIGHLIGHT} uses it to decide how far down the plan it may
 * look for the {@code WHERE} that supplies its implicit query: as long as every node in between is doc-preserving, a predicate that
 * selected a document still describes the row that reaches {@code HIGHLIGHT}.
 * <p>
 * Commands that are <b>not</b> doc-preserving:
 * <ul>
 *     <li>{@link Aggregate} and {@link InlineStats}: a row summarizes many documents.</li>
 *     <li>{@link Fuse}: merges rows originating from different branches.</li>
 *     <li>{@link Fork} and {@link Join}: rows come from more than one input, so "the" document is ambiguous.</li>
 *     <li>Nodes that synthesize rows rather than carry them through, such as {@link InsertEmptyBuckets}.</li>
 * </ul>
 * <p>
 * Implement this on the command itself rather than listing command classes in the consumer: a new command then has to make the call at
 * the point where its row semantics are being decided, instead of silently defaulting to whatever a list somewhere else happens to say.
 */
public interface DocPreserving {}
