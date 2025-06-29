/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

/**
 * A per shard stats including the number of segments and total fields across those segments.
 * These stats should be recomputed whenever the shard is refreshed.
 *
 * @param numSegments the number of segments
 * @param totalFields the total number of fields across the segments
 * @param fieldUsages the number of usages for segment-level fields (e.g., doc_values, postings, norms, points)
 *                    -1 if unavailable
 * @param postingsInMemoryBytes the total bytes in memory used for postings across all fields
 */
public record ShardFieldStats(int numSegments, int totalFields, long fieldUsages, long postingsInMemoryBytes) {

}
