/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.transport.ResponseReservation;

/**
 * Breaker labels under which a search charges the shard responses it receives, see {@link ResponseReservation}. For a shard response, a
 * trip means this node has no room for the response, not that the shard failed, so the search fails as a whole instead of retrying the
 * shard on another copy.
 */
public final class SearchResponseBreakerLabels {

    /** Label for a fetch response, which carries hits. */
    public static final String FETCH_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_FETCH + "[shard_response]";

    /** Label for the responses of the other phases. */
    public static final String BREAKER_LABEL = "<shard_response>";

    private SearchResponseBreakerLabels() {}
}
