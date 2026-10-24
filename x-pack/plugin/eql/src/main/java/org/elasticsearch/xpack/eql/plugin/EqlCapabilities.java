/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plugin;

import java.util.HashSet;
import java.util.Set;

public final class EqlCapabilities {

    private EqlCapabilities() {}

    /** Fix bug on filters that include join keys https://github.com/elastic/elasticsearch/issues/133065 */
    private static final String FILTERS_ON_KEYS_FIX = "filters_on_keys_fix";

    /** Invalid timestamp or tiebreaker values fail sequences with 4xx, not 5xx https://github.com/elastic/elasticsearch/issues/156605 */
    private static final String INVALID_TIMESTAMP_TIEBREAKER_4XX = "invalid_timestamp_tiebreaker_4xx";

    public static final Set<String> CAPABILITIES;
    static {
        HashSet<String> capabilities = new HashSet<>();
        capabilities.add(FILTERS_ON_KEYS_FIX);
        capabilities.add(INVALID_TIMESTAMP_TIEBREAKER_4XX);
        CAPABILITIES = Set.copyOf(capabilities);
    }
}
