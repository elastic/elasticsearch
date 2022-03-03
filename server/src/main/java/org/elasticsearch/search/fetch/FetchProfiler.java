/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Timer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class FetchProfiler implements FetchPhase.Profiler {
    private final FetchProfileBreakdown current;

    /**
     * Start profiling at the current time.
     */
    public FetchProfiler() {
        this(System.nanoTime());
    }

    /**
     * Build the profiler starting at a fixed time.
     */
    public FetchProfiler(long nanoTime) {
        current = new FetchProfileBreakdown(nanoTime);
    }

    /**
     * Finish profiling at the current time.
     */
    @Override
    public ProfileResult finish() {
        return finish(System.nanoTime());
    }

    /**
     * Finish profiling at a fixed time.
     */
    public ProfileResult finish(long nanoTime) {
        return current.result(nanoTime);
    }

    @Override
    public void visitor(FieldsVisitor fieldsVisitor) {
        current.debug.put(
            "stored_fields",
            fieldsVisitor == null ? List.of() : fieldsVisitor.getFieldNames().stream().sorted().collect(toList())
        );
    }

    @Override
    public FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor delegate) {
        FetchSubPhaseProfileBreakdown breakdown = new FetchSubPhaseProfileBreakdown(type, description, delegate);
        current.subPhases.add(breakdown);
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                Timer timer = breakdown.getTimer(FetchSubPhaseTiming.NEXT_READER);
                timer.start();
                try {
                    delegate.setNextReader(readerContext);
                } finally {
                    timer.stop();
                }
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Timer timer = breakdown.getTimer(FetchSubPhaseTiming.PROCESS);
                timer.start();
                try {
                    delegate.process(hitContext);
                } finally {
                    timer.stop();
                }
            }
        };
    }

    @Override
    public void startLoadingStoredFields() {
        current.getTimer(FetchPhaseTiming.LOAD_STORED_FIELDS).start();
    }

    @Override
    public void stopLoadingStoredFields() {
        current.getTimer(FetchPhaseTiming.LOAD_STORED_FIELDS).stop();
    }

    @Override
    public void startNextReader() {
        current.getTimer(FetchPhaseTiming.NEXT_READER).start();
    }

    @Override
    public void stopNextReader() {
        current.getTimer(FetchPhaseTiming.NEXT_READER).stop();
    }

    static class FetchProfileBreakdown extends AbstractProfileBreakdown<FetchPhaseTiming> {
        private final long start;
        private final Map<String, Object> debug = new HashMap<>();
        private final List<FetchSubPhaseProfileBreakdown> subPhases = new ArrayList<>();

        FetchProfileBreakdown(long start) {
            super(FetchPhaseTiming.class);
            this.start = start;
        }

        @Override
        protected Map<String, Object> toDebugMap() {
            return Map.copyOf(debug);
        }

        ProfileResult result(long stop) {
            List<ProfileResult> children = subPhases.stream()
                .sorted(Comparator.comparing(b -> b.type))
                .map(FetchSubPhaseProfileBreakdown::result)
                .collect(toList());
            return new ProfileResult("fetch", "", toBreakdownMap(), toDebugMap(), stop - start, children);
        }
    }

    enum FetchPhaseTiming {
        NEXT_READER,
        LOAD_STORED_FIELDS;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    static class FetchSubPhaseProfileBreakdown extends AbstractProfileBreakdown<FetchSubPhaseTiming> {
        private final String type;
        private final String description;
        private final FetchSubPhaseProcessor processor;

        FetchSubPhaseProfileBreakdown(String type, String description, FetchSubPhaseProcessor processor) {
            super(FetchSubPhaseTiming.class);
            this.type = type;
            this.description = description;
            this.processor = processor;
        }

        @Override
        protected Map<String, Object> toDebugMap() {
            return processor.getDebugInfo();
        }

        ProfileResult result() {
            return new ProfileResult(type, description, toBreakdownMap(), toDebugMap(), toNodeTime(), List.of());
        }
    }

    enum FetchSubPhaseTiming {
        NEXT_READER,
        PROCESS;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

}
