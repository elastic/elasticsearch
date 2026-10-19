/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;

import java.util.HashMap;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 */
public final class QueryProfileBreakdown extends AbstractProfileBreakdown<QueryTimingType> {

    private Integer rescorer;

    /** Sole constructor. */
    public QueryProfileBreakdown() {
        super(QueryTimingType.class);
    }

    /**
     * Record that this query was run by the rescorer at the given position of the rescore phase.
     */
    void setRescorer(int rescorer) {
        this.rescorer = rescorer;
    }

    @Override
    protected Map<String, Object> toDebugMap() {
        Map<String, Object> debug = super.toDebugMap();
        if (rescorer == null) {
            return debug;
        }
        Map<String, Object> withRescorer = new HashMap<>(debug);
        withRescorer.put("rescorer", rescorer);
        return withRescorer;
    }
}
