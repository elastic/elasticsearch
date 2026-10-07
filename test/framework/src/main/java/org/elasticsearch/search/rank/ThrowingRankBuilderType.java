/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

public enum ThrowingRankBuilderType {
    THROWING_QUERY_PHASE_SHARD_CONTEXT,
    THROWING_QUERY_PHASE_COORDINATOR_CONTEXT,
    THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT,
    THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT;
}
