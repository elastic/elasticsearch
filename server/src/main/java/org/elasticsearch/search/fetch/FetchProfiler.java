/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
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
    public StoredFieldLoader storedFields(StoredFieldLoader storedFieldLoader) {
        current.debug.put("stored_fields", storedFieldLoader.fieldsToLoad());
        return new StoredFieldLoader() {
            @Override
            public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) {
                LeafStoredFieldLoader in = storedFieldLoader.getLoader(ctx, docs);
                return new LeafStoredFieldLoader() {
                    @Override
                    public void advanceTo(int doc) throws IOException {
                        current.getTimer(FetchPhaseTiming.LOAD_STORED_FIELDS).start();
                        try {
                            in.advanceTo(doc);
                        } finally {
                            current.getTimer(FetchPhaseTiming.LOAD_STORED_FIELDS).stop();
                        }
                    }

                    @Override
                    public BytesReference source() {
                        return in.source();
                    }

                    @Override
                    public String id() {
                        return in.id();
                    }

                    @Override
                    public String routing() {
                        return in.routing();
                    }

                    @Override
                    public Map<String, List<Object>> storedFields() {
                        return in.storedFields();
                    }
                };
            }

            @Override
            public List<String> fieldsToLoad() {
                return storedFieldLoader.fieldsToLoad();
            }
        };
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
    public void startLoadingSource() {
        current.getTimer(FetchPhaseTiming.LOAD_SOURCE).start();
    }

    @Override
    public void stopLoadingSource() {
        current.getTimer(FetchPhaseTiming.LOAD_SOURCE).stop();
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

    /**
     * Actions within the "main" fetch phase that are explicitly profiled.
     * See also {@link FetchSubPhaseProfileBreakdown}.
     */
    enum FetchPhaseTiming {
        /**
         * Time spent setting up infrastructure for each segment. This is
         * called once per segment that has a matching document.
         */
        NEXT_READER,
        /**
         * Time spent loading stored fields for each document. This is called
         * once per document if the fetch needs stored fields. Most do.
         */
        LOAD_STORED_FIELDS,
        /**
         * Time spent computing the {@code _source}. This is called once per
         * document that needs to fetch source. This may be as fast as reading
         * {@code _source} from the stored fields or as slow as loading doc
         * values for all fields.
         */
        LOAD_SOURCE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Timings from an optional sub-phase of fetch.
     */
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
