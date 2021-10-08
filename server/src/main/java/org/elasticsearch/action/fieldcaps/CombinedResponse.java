/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

final class CombinedResponse {
    private final MergeResultsMode mergeMode;

    // fully merged
    private final List<String> indices;
    private final Map<String, Map<String, FieldCapabilities>> responseMap;

    // partially merged
    private final Map<String, Map<String, FieldCapabilities.Builder>> responseBuilder;

    // no merge
    private final List<FieldCapabilitiesIndexResponse> indexResponses;

    private final List<FieldCapabilitiesFailure> indexFailures;

    private CombinedResponse(MergeResultsMode mergeMode, List<String> indices,
                             Map<String, Map<String, FieldCapabilities>> responseMap,
                             Map<String, Map<String, FieldCapabilities.Builder>> responseBuilder,
                             List<FieldCapabilitiesIndexResponse> indexResponses,
                             List<FieldCapabilitiesFailure> indexFailures) {
        switch (mergeMode) {
            case FULL_MERGE:
                if (indexResponses.isEmpty() == false || responseBuilder.isEmpty() == false) {
                    throw new IllegalStateException("Response wasn't fully merged");
                }
                break;
            case INTERNAL_PARTIAL_MERGE:
                if (indexResponses.isEmpty() == false) {
                    throw new IllegalStateException("Response wasn't merged");
                }
                if (responseMap.isEmpty() == false) {
                    throw new IllegalStateException("Response was fully merged");
                }
                break;
            case NO_MERGE:
                if (indices.isEmpty() == false || responseMap.isEmpty() == false) {
                    throw new IllegalStateException("Response was fully merged");
                }
                if (responseBuilder.isEmpty() == false) {
                    throw new IllegalStateException("Response was partially merged");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown merge option " + mergeMode);
        }
        this.mergeMode = mergeMode;
        this.indices = indices;
        this.responseMap = responseMap;
        this.responseBuilder = responseBuilder;
        this.indexResponses = indexResponses;
        this.indexFailures = indexFailures;
    }

    FieldCapabilitiesResponse toFieldCapResponse() {
        switch (mergeMode) {
            case NO_MERGE:
                return new FieldCapabilitiesResponse(indexResponses, indexFailures);
            case FULL_MERGE:
                return new FieldCapabilitiesResponse(indices.toArray(new String[0]), responseMap, indexFailures);
            default:
                throw new IllegalArgumentException("Unsupported merge option " + mergeMode);
        }
    }

    FieldCapabilitiesNodeResponse toNodeResponse(Set<ShardId> unmatchedShardIds) {
        switch (mergeMode) {
            case NO_MERGE:
                return new FieldCapabilitiesNodeResponse(indexResponses, indexFailures, unmatchedShardIds);
            case INTERNAL_PARTIAL_MERGE:
                return new FieldCapabilitiesNodeResponse(indices, responseBuilder, indexFailures, unmatchedShardIds);
            default:
                throw new IllegalArgumentException("Unsupported merge option " + mergeMode);
        }
    }

    MergeResultsMode getMergeMode() {
        return mergeMode;
    }

    List<String> getIndices() {
        return indices;
    }

    List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    Map<String, Map<String, FieldCapabilities>> getResponseMap() {
        return responseMap;
    }

    Map<String, Map<String, FieldCapabilities.Builder>> getResponseBuilder() {
        return responseBuilder;
    }

    List<FieldCapabilitiesFailure> getIndexFailures() {
        return indexFailures;
    }

    /**
     * Creates an async builder to generate a response from multiple input responses. After adding all inputs, the caller needs to call
     * {@link Builder#complete()} to finalize the combined result and notify it via the listener when ready.
     *
     * @param mergeMode              the merge mode
     * @param includeUnmapped        only affect when the merge mode is {@link MergeResultsMode#FULL_MERGE}.
     * @param metadataFieldPredicate best-effort to guess a field is a metadata-field when an input response is from an old node
     * @param executor               the executor that is used to combine input responses
     * @param listener               the result will be notified via the listener
     */
    static Builder newBuilder(MergeResultsMode mergeMode, boolean includeUnmapped, Predicate<String> metadataFieldPredicate,
                              Executor executor, ActionListener<CombinedResponse> listener) {
        if (mergeMode == MergeResultsMode.NO_MERGE) {
            return new NoMergeBuilder(listener);
        } else {
            return new MergedBuilder(mergeMode, includeUnmapped, metadataFieldPredicate, executor, listener);
        }
    }

    static abstract class Builder {
        private final Map<String, Exception> failures = ConcurrentCollections.newConcurrentMap();
        protected final ActionListener<CombinedResponse> listener;

        Builder(ActionListener<CombinedResponse> listener) {
            this.listener = listener;
        }

        /**
         * The caller needs to call this method after adding all input responses and failures
         */
        abstract void complete();

        /**
         * Adds a partially merged response. Only Builders created with {@link MergeResultsMode#INTERNAL_PARTIAL_MERGE}
         * and {@link MergeResultsMode#FULL_MERGE} support merging partially merge responses.
         */
        abstract void addMergedResponse(List<String> indices, Map<String, Map<String, FieldCapabilities.Builder>> responseBuilder);

        /**
         * Adds a list of  un-merged index responses.
         */
        abstract void addIndexResponses(List<FieldCapabilitiesIndexResponse> indexResponses);

        final void addIndexFailure(String index, Exception e) {
            failures.put(index, e);
        }

        final List<FieldCapabilitiesFailure> getIndexFailures(Set<String> indicesWithResponses) {
            Map<Tuple<String, String>, FieldCapabilitiesFailure> indexFailures = Collections.synchronizedMap(new HashMap<>());
            for (Map.Entry<String, Exception> failure : failures.entrySet()) {
                String index = failure.getKey();
                Exception e = failure.getValue();
                if (indicesWithResponses.contains(index) == false) {
                    // we deduplicate exceptions on the underlying causes message and classname
                    // we unwrap the cause to e.g. group RemoteTransportExceptions coming from different nodes if the cause is the same
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    Tuple<String, String> groupingKey = new Tuple<>(cause.getMessage(), cause.getClass().getName());
                    indexFailures.compute(groupingKey,
                        (k, v) -> v == null ? new FieldCapabilitiesFailure(new String[]{index}, e) : v.addIndex(index));
                }
            }
            return new ArrayList<>(indexFailures.values());
        }
    }

    private static class NoMergeBuilder extends Builder {
        private final Map<String, FieldCapabilitiesIndexResponse> indexResponses = ConcurrentCollections.newConcurrentMap();

        NoMergeBuilder(ActionListener<CombinedResponse> listener) {
            super(listener);
        }

        @Override
        void complete() {
            final List<FieldCapabilitiesFailure> indexFailures = getIndexFailures(indexResponses.keySet());
            final CombinedResponse combinedResponse = new CombinedResponse(MergeResultsMode.NO_MERGE, Collections.emptyList(),
                Collections.emptyMap(), Collections.emptyMap(), new ArrayList<>(indexResponses.values()), indexFailures);
            listener.onResponse(combinedResponse);
        }

        @Override
        void addMergedResponse(List<String> indices, Map<String, Map<String, FieldCapabilities.Builder>> responseBuilder) {
            throw new UnsupportedOperationException("NO_MERGE builder can't handle a merged response");
        }

        @Override
        void addIndexResponses(List<FieldCapabilitiesIndexResponse> responses) {
            for (FieldCapabilitiesIndexResponse response : responses) {
                if (response.canMatch()) {
                    indexResponses.put(response.getIndexName(), response);
                }
            }
        }
    }

    private static class MergedBuilder extends Builder {
        private final MergeResultsMode mergeMode;
        private final boolean includeUnmapped;
        private final Predicate<String> metadataFieldPredicate;
        private final Executor executor;
        private final Set<String> indicesWithResponses;
        private final List<String> orderedIndices = new ArrayList<>();
        private final Map<String, Map<String, FieldCapabilities.Builder>> fieldToTypeToCapBuilders;
        private final AtomicInteger pendingEvents = new AtomicInteger(1);

        MergedBuilder(MergeResultsMode mergeMode, boolean includeUnmapped, Predicate<String> metadataFieldPredicate,
                      Executor executor, ActionListener<CombinedResponse> listener) {
            super(listener);
            this.mergeMode = mergeMode;
            this.includeUnmapped = includeUnmapped;
            this.metadataFieldPredicate = metadataFieldPredicate;
            this.executor = executor;
            this.indicesWithResponses = ConcurrentCollections.newConcurrentSet();
            this.fieldToTypeToCapBuilders = ConcurrentCollections.newConcurrentMap();
        }

        private int addGlobalIndex(String index) {
            synchronized (orderedIndices) {
                final int pos = orderedIndices.size();
                orderedIndices.add(index);
                return pos;
            }
        }

        private void countDown() {
            if (pendingEvents.decrementAndGet() == 0) {
                ActionListener.completeWith(listener, () -> {
                    final List<FieldCapabilitiesFailure> indexFailures = getIndexFailures(indicesWithResponses);
                    final CombinedResponse combinedResponse;
                    if (mergeMode == MergeResultsMode.INTERNAL_PARTIAL_MERGE) {
                        combinedResponse = new CombinedResponse(mergeMode, orderedIndices, Collections.emptyMap(),
                            fieldToTypeToCapBuilders, Collections.emptyList(), indexFailures);
                    } else {
                        assert mergeMode == MergeResultsMode.FULL_MERGE;
                        final Map<String, Map<String, FieldCapabilities>> responseMap = fullMerge();
                        fieldToTypeToCapBuilders.clear();
                        combinedResponse = new CombinedResponse(mergeMode, orderedIndices, responseMap,
                            Collections.emptyMap(), Collections.emptyList(), indexFailures);
                    }
                    return combinedResponse;
                });
            }
        }

        private Map<String, Map<String, FieldCapabilities>> fullMerge() {
            final Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
            for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> e : fieldToTypeToCapBuilders.entrySet()) {
                final String fieldName = e.getKey();
                final Map<String, FieldCapabilities.Builder> typeToCapBuilders = e.getValue();
                if (includeUnmapped) {
                    addUnmappedFields(orderedIndices, typeToCapBuilders);
                }
                final boolean multiTypes = typeToCapBuilders.size() > 1;
                final Map<String, FieldCapabilities> typeMap = new HashMap<>();
                for (Map.Entry<String, FieldCapabilities.Builder> fieldEntry : typeToCapBuilders.entrySet()) {
                    final String fieldType = fieldEntry.getKey();
                    typeMap.put(fieldEntry.getKey(), fieldEntry.getValue().build(fieldName, fieldType, multiTypes));
                }
                typeToCapBuilders.clear();
                responseMap.put(fieldName, Collections.unmodifiableMap(typeMap));
            }
            return responseMap;
        }

        private static void addUnmappedFields(List<String> indices, Map<String, FieldCapabilities.Builder> typeMap) {
            Set<String> unmappedIndices = new HashSet<>(indices);
            typeMap.values().forEach((b) -> b.getIndices().forEach(unmappedIndices::remove));
            if (unmappedIndices.isEmpty() == false) {
                FieldCapabilities.Builder unmapped = new FieldCapabilities.Builder();
                typeMap.put("unmapped", unmapped);
                for (String index : unmappedIndices) {
                    unmapped.add(index, false, false, false, Collections.emptyMap());
                }
            }
        }

        @Override
        void complete() {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() {
                    countDown();
                }
            });
        }

        private void innerAddMergedResponse(List<String> indices, Map<String, Map<String, FieldCapabilities.Builder>> response) {
            Set<String> ignoredIndices = new HashSet<>();
            for (String index : indices) {
                if (indicesWithResponses.add(index)) {
                    addGlobalIndex(index);
                } else {
                    ignoredIndices.add(index);
                }
            }
            for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> fieldEntry : response.entrySet()) {
                final String field = fieldEntry.getKey();
                final Map<String, FieldCapabilities.Builder> typeToCaps =
                    fieldToTypeToCapBuilders.computeIfAbsent(field, f -> ConcurrentCollections.newConcurrentMap());
                for (Map.Entry<String, FieldCapabilities.Builder> typeEntry : fieldEntry.getValue().entrySet()) {
                    final String type = typeEntry.getKey();
                    final FieldCapabilities.Builder typeCapBuilder = typeEntry.getValue();
                    typeToCaps.computeIfAbsent(type, t -> new FieldCapabilities.Builder()).add(typeCapBuilder, ignoredIndices);
                }
            }
        }

        @Override
        void addMergedResponse(List<String> indices, Map<String, Map<String, FieldCapabilities.Builder>> responseBuilder) {
            pendingEvents.incrementAndGet();
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    for (String index : indices) {
                        addIndexFailure(index, e);
                    }
                }

                @Override
                protected void doRun() {
                    innerAddMergedResponse(indices, responseBuilder);
                }

                @Override
                public void onAfter() {
                    countDown();
                }
            });
        }

        private void addIndexResponse(FieldCapabilitiesIndexResponse response) {
            final String index = response.getIndexName();
            if (response.canMatch() && indicesWithResponses.add(index)) {
                int pos = addGlobalIndex(index);
                final Map<String, IndexFieldCapabilities> fieldToCaps = response.get();
                for (Map.Entry<String, IndexFieldCapabilities> e : fieldToCaps.entrySet()) {
                    final IndexFieldCapabilities cap = e.getValue();
                    final String field = e.getKey();
                    final Map<String, FieldCapabilities.Builder> typeToCapBuilders = fieldToTypeToCapBuilders
                        .computeIfAbsent(field, f -> ConcurrentCollections.newConcurrentMap());
                    final FieldCapabilities.Builder builder =
                        typeToCapBuilders.computeIfAbsent(cap.getType(), t -> new FieldCapabilities.Builder());
                    final boolean isMetadataField;
                    if (response.getOriginVersion().onOrAfter(Version.V_7_13_0)) {
                        isMetadataField = cap.isMetadatafield();
                    } else {
                        // best effort to detect metadata field coming from older nodes
                        isMetadataField = metadataFieldPredicate.test(field);
                    }
                    builder.add(index, isMetadataField, cap.isSearchable(), cap.isAggregatable(), cap.meta());
                }
            }
        }

        @Override
        void addIndexResponses(List<FieldCapabilitiesIndexResponse> responses) {
            pendingEvents.incrementAndGet();
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    for (FieldCapabilitiesIndexResponse response : responses) {
                        addIndexFailure(response.getIndexName(), e);
                    }
                }

                @Override
                protected void doRun() {
                    for (FieldCapabilitiesIndexResponse r : responses) {
                        addIndexResponse(r);
                    }
                }

                @Override
                public void onAfter() {
                    countDown();
                }
            });
        }
    }
}
