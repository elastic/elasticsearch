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
import java.util.TreeSet;

import static java.util.stream.Collectors.toList;

public class FetchProfiler implements FetchPhase.Profiler {
    private FetchProfileBreakdown current;

    private long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public void start() {
        assert current == null;
        current = new FetchProfileBreakdown(nanoTime());
    }

    @Override
    public ProfileResult stop() {
        ProfileResult result = current.result(nanoTime());
        current = null;
        return result;
    }

    @Override
    public void visitor(FieldsVisitor fieldsVisitor) {
        current.debug.put("stored_fields", fieldsVisitor == null ? List.of() : new TreeSet<>(fieldsVisitor.getFieldNames()));
    }

    @Override
    public FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor delegate) {
        FetchSubPhaseProfileBreakdown breakdown = new FetchSubPhaseProfileBreakdown(type, description);
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

            @Override
            public void done() {
                breakdown.debug = delegate.getDebugInfo();
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
            List<ProfileResult> subPhases = this.subPhases.stream()
                .sorted(Comparator.comparing(b -> b.type))
                .map(FetchSubPhaseProfileBreakdown::result)
                .collect(toList());
            return new ProfileResult("fetch", "fetch", toBreakdownMap(), toDebugMap(), stop - start, subPhases);
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
        private Map<String, Object> debug;

        FetchSubPhaseProfileBreakdown(String type, String description) {
            super(FetchSubPhaseTiming.class);
            this.type = type;
            this.description = description;
        }

        @Override
        protected Map<String, Object> toDebugMap() {
            return debug;
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
