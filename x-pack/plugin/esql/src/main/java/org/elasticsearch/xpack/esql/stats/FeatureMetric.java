/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import java.util.Locale;

public enum FeatureMetric {

    /**
     * The order of these enum values is important, do not change it.
     * For any new values added to it, they should go at the end of the list.
     * see {@link org.elasticsearch.xpack.esql.analysis.Verifier#gatherMetrics}
     */
    DISSECT,
    EVAL,
    GROK,
    LIMIT,
    SORT,
    STATS,
    WHERE;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
