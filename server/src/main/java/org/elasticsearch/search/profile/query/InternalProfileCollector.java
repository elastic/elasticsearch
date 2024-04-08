/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.sandbox.search.ProfilerCollector;
import org.apache.lucene.search.Collector;
import org.elasticsearch.search.internal.TwoPhaseCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class wraps a Lucene Collector and times the execution of:
 * - setScorer()
 * - collect()
 * - doSetNextReader()
 * - needsScores()
 * <p>
 * InternalProfiler facilitates the linking of the Collector graph
 */
public class InternalProfileCollector extends ProfilerCollector implements TwoPhaseCollector {

    private final InternalProfileCollector[] children;
    private final Collector wrappedCollector;

    public InternalProfileCollector(Collector collector, String reason, InternalProfileCollector... children) {
        super(collector, reason, Arrays.asList(children));
        this.wrappedCollector = collector;
        this.children = children;
    }

    public Collector getWrappedCollector() {
        return wrappedCollector;
    }

    /**
     * Creates a human-friendly representation of the Collector name.
     * <p>
     * InternalBucket Collectors use the aggregation name in their toString() method,
     * which makes the profiled output a bit nicer.
     *
     * @param c The Collector to derive a name from
     * @return  A (hopefully) prettier name
     */
    @Override
    protected String deriveCollectorName(Collector c) {
        String s = c.getClass().getSimpleName();

        // MultiCollector which wraps multiple BucketCollectors is generated
        // via an anonymous class, so this corrects the lack of a name by
        // asking the enclosingClass
        if (s.equals("")) {
            s = c.getClass().getEnclosingClass().getSimpleName();
        }

        // Aggregation collector toString()'s include the user-defined agg name
        if (getReason().equals(CollectorResult.REASON_AGGREGATION) || getReason().equals(CollectorResult.REASON_AGGREGATION_GLOBAL)) {
            s += ": " + c;
        }
        return s;
    }

    public CollectorResult getCollectorTree() {
        List<CollectorResult> childResults = new ArrayList<>(children.length);
        for (InternalProfileCollector child : children) {
            CollectorResult result = child.getCollectorTree();
            childResults.add(result);
        }
        return new CollectorResult(getName(), getReason(), getTime(), childResults);
    }

    @Override
    public void doPostCollection() throws IOException {
        if (wrappedCollector instanceof TwoPhaseCollector twoPhaseCollector) {
            twoPhaseCollector.doPostCollection();
        }
    }
}
