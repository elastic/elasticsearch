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
import org.elasticsearch.search.profile.CodecReads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 */
public final class QueryProfileBreakdown extends AbstractProfileBreakdown<QueryTimingType> {

    private Integer rescorer;

    /** The codec reads of each weight built for this node. A query can build more than one weight. */
    private final List<CodecReadProfiling> codecReads = new ArrayList<>();

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

    /** Adds the codec reads of one weight. They are merged with the others when the profile result is built. */
    void addCodecReads(CodecReadProfiling reads) {
        synchronized (codecReads) {
            codecReads.add(reads);
        }
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

    @Override
    protected CodecReads toCodecReads() {
        final List<CodecReads> parts = new ArrayList<>();
        synchronized (codecReads) {
            for (CodecReadProfiling reads : codecReads) {
                parts.add(reads.snapshot());
            }
        }
        return parts.isEmpty() ? CodecReads.EMPTY : CodecReads.merge(parts);
    }
}
