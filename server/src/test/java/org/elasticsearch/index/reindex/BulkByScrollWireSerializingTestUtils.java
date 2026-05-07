/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.randomPitWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.ScrollWorkerResumeInfoWireSerializingTests.randomScrollWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.sliceMapContentHashCode;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.sliceMapsContentEqual;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.workerResumeInfoContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.workerResumeInfoContentHashCode;
import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

/**
 * Shared helpers for bulk-by-scroll wire serialization tests.
 */
public final class BulkByScrollWireSerializingTestUtils {

    private BulkByScrollWireSerializingTestUtils() {}

    /**
     * Registry sufficient for {@link ReindexRequest}, {@link UpdateByQueryRequest}, {@link DeleteByQueryRequest},
     * and {@link ResumeBulkByScrollRequest} when the delegate carries {@link ResumeInfo}.
     */
    public static NamedWriteableRegistry bulkScrollRequestNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        entries.addAll(searchModule.getNamedWriteables());
        entries.addAll(ClusterModule.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new));
        entries.add(
            new NamedWriteableRegistry.Entry(
                ResumeInfo.WorkerResumeInfo.class,
                ResumeInfo.ScrollWorkerResumeInfo.NAME,
                ResumeInfo.ScrollWorkerResumeInfo::new
            )
        );
        entries.add(
            new NamedWriteableRegistry.Entry(
                ResumeInfo.WorkerResumeInfo.class,
                ResumeInfo.PitWorkerResumeInfo.NAME,
                ResumeInfo.PitWorkerResumeInfo::new
            )
        );
        return new NamedWriteableRegistry(entries);
    }

    public static boolean abstractBulkByScrollRequestsEqual(
        AbstractBulkByScrollRequest<?> firstRequest,
        AbstractBulkByScrollRequest<?> secondRequest
    ) {
        if (Objects.equals(firstRequest.getSearchRequest(), secondRequest.getSearchRequest()) == false) {
            return false;
        }
        if (firstRequest.getMaxDocs() != secondRequest.getMaxDocs()) {
            return false;
        }
        if (firstRequest.isAbortOnVersionConflict() != secondRequest.isAbortOnVersionConflict()) {
            return false;
        }
        if (firstRequest.isRefresh() != secondRequest.isRefresh()) {
            return false;
        }
        if (Objects.equals(firstRequest.getTimeout(), secondRequest.getTimeout()) == false) {
            return false;
        }
        if (Objects.equals(firstRequest.getWaitForActiveShards(), secondRequest.getWaitForActiveShards()) == false) {
            return false;
        }
        if (Objects.equals(firstRequest.getRetryBackoffInitialTime(), secondRequest.getRetryBackoffInitialTime()) == false) {
            return false;
        }
        if (firstRequest.getMaxRetries() != secondRequest.getMaxRetries()) {
            return false;
        }
        if (Float.compare(firstRequest.getRequestsPerSecond(), secondRequest.getRequestsPerSecond()) != 0) {
            return false;
        }
        if (firstRequest.getSlices() != secondRequest.getSlices()) {
            return false;
        }
        if (firstRequest.getShouldStoreResult() != secondRequest.getShouldStoreResult()) {
            return false;
        }
        if (firstRequest.isEligibleForRelocationOnShutdown() != secondRequest.isEligibleForRelocationOnShutdown()) {
            return false;
        }
        if (resumeInfosEqual(firstRequest.getResumeInfo(), secondRequest.getResumeInfo()) == false) {
            return false;
        }
        return arraysEqual(firstRequest.getSourceIndicesForDescription(), secondRequest.getSourceIndicesForDescription());
    }

    /**
     * Equality for {@link AbstractBulkByScrollRequest#getResumeInfo()} suitable for wire tests: {@link ResumeInfo.PitWorkerResumeInfo}
     * uses {@code searchAfterValues} that may change numeric types across {@link org.elasticsearch.common.io.stream.StreamOutput} /
     * {@link org.elasticsearch.common.io.stream.StreamInput} round-trips.
     */
    public static boolean resumeInfosEqual(Optional<ResumeInfo> first, Optional<ResumeInfo> second) {
        if (first.isPresent() != second.isPresent()) {
            return false;
        }
        return first.isEmpty() || resumeInfoContentEquals(first.get(), second.get());
    }

    /**
     * Hash code consistent with {@link #resumeInfosEqual(Optional, Optional)} for wire-test wrappers.
     */
    public static int resumeInfoOptionalContentHashCode(Optional<ResumeInfo> resumeInfo) {
        return resumeInfo.isEmpty() ? 0 : resumeInfoContentHashCode(resumeInfo.get());
    }

    private static boolean resumeInfoContentEquals(ResumeInfo first, ResumeInfo second) {
        if (Objects.equals(first.relocationOrigin(), second.relocationOrigin()) == false) {
            return false;
        }
        if (Objects.equals(first.sourceTaskResult(), second.sourceTaskResult()) == false) {
            return false;
        }
        if (first.worker() != null) {
            return second.worker() != null
                && first.slices() == null
                && second.slices() == null
                && workerResumeInfoContentEquals(first.worker(), second.worker());
        }
        if (first.slices() != null) {
            return second.slices() != null && sliceMapsContentEqual(first.slices(), second.slices());
        }
        return second.worker() == null && second.slices() == null;
    }

    private static int resumeInfoContentHashCode(ResumeInfo resumeInfo) {
        int result = Objects.hashCode(resumeInfo.relocationOrigin());
        result = 31 * result + Objects.hashCode(resumeInfo.sourceTaskResult());
        if (resumeInfo.worker() != null) {
            result = 31 * result + workerResumeInfoContentHashCode(resumeInfo.worker());
        } else if (resumeInfo.slices() != null) {
            result = 31 * result + sliceMapContentHashCode(resumeInfo.slices());
        }
        return result;
    }

    private static boolean arraysEqual(String[] firstIndices, String[] secondIndices) {
        if (firstIndices == null && secondIndices == null) {
            return true;
        }
        if (firstIndices == null || secondIndices == null) {
            return false;
        }
        if (firstIndices.length != secondIndices.length) {
            return false;
        }
        for (int index = 0; index < firstIndices.length; index++) {
            if (Objects.equals(firstIndices[index], secondIndices[index]) == false) {
                return false;
            }
        }
        return true;
    }

    public static boolean abstractBulkIndexByScrollRequestsEqual(
        AbstractBulkIndexByScrollRequest<?> firstRequest,
        AbstractBulkIndexByScrollRequest<?> secondRequest
    ) {
        if (abstractBulkByScrollRequestsEqual(firstRequest, secondRequest) == false) {
            return false;
        }
        return Objects.equals(firstRequest.getScript(), secondRequest.getScript());
    }

    public static boolean reindexRequestsEqual(ReindexRequest firstRequest, ReindexRequest secondRequest) {
        if (abstractBulkIndexByScrollRequestsEqual(firstRequest, secondRequest) == false) {
            return false;
        }
        if (Objects.equals(firstRequest.getDestination().index(), secondRequest.getDestination().index()) == false) {
            return false;
        }
        if (firstRequest.getDestination().version() != secondRequest.getDestination().version()) {
            return false;
        }
        if (Objects.equals(firstRequest.getRemoteInfo(), secondRequest.getRemoteInfo()) == false) {
            return false;
        }
        return true;
    }

    public static boolean updateByQueryRequestsEqual(UpdateByQueryRequest firstRequest, UpdateByQueryRequest secondRequest) {
        if (abstractBulkIndexByScrollRequestsEqual(firstRequest, secondRequest) == false) {
            return false;
        }
        return Objects.equals(firstRequest.getPipeline(), secondRequest.getPipeline());
    }

    public static boolean deleteByQueryRequestsEqual(DeleteByQueryRequest firstRequest, DeleteByQueryRequest secondRequest) {
        return abstractBulkByScrollRequestsEqual(firstRequest, secondRequest);
    }

    public static BytesReference matchAllQueryBytes() {
        try {
            return BytesReference.bytes(matchAllQuery().toXContent(XContentBuilder.builder(jsonXContent), ToXContent.EMPTY_PARAMS));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    /**
     * Random {@link RemoteInfo} with valid JSON query bytes.
     */
    public static RemoteInfo randomRemoteInfo() {
        BytesReference query = matchAllQueryBytes();
        return new RemoteInfo(
            ESTestCase.randomAlphaOfLength(5),
            ESTestCase.randomAlphaOfLength(5),
            ESTestCase.between(1, 65535),
            ESTestCase.randomBoolean() ? ESTestCase.randomAlphaOfLength(4) : null,
            query,
            ESTestCase.randomBoolean() ? ESTestCase.randomAlphaOfLength(5) : null,
            ESTestCase.randomBoolean() ? new SecureString(ESTestCase.randomAlphaOfLength(5).toCharArray()) : null,
            ESTestCase.randomBoolean()
                ? Map.of(ESTestCase.randomAlphaOfLength(3), ESTestCase.randomAlphaOfLength(3))
                : Collections.emptyMap(),
            ESTestCase.randomTimeValue(),
            ESTestCase.randomTimeValue()
        );
    }

    /**
     * Minimal random resume info for embedding in bulk-by-scroll requests (worker or multi-slice).
     */
    public static void fillRandomBulkFields(AbstractBulkByScrollRequest<?> request) {
        if (ESTestCase.randomBoolean()) {
            request.setMaxDocs(ESTestCase.between(100, 10000));
        }
        // Else leave maxDocs at MAX_DOCS_ALL_MATCHES (-1): setMaxDocs rejects negatives, so "unlimited" is only the default field value.
        request.setAbortOnVersionConflict(ESTestCase.randomBoolean());
        request.setRefresh(ESTestCase.rarely());
        request.setTimeout(ESTestCase.randomTimeValue());
        request.setWaitForActiveShards(
            ESTestCase.randomFrom(ActiveShardCount.ALL, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.DEFAULT)
        );
        request.setRetryBackoffInitialTime(ESTestCase.randomPositiveTimeValue());
        request.setMaxRetries(ESTestCase.between(0, 20));
        request.setRequestsPerSecond(
            ESTestCase.randomBoolean() ? Float.POSITIVE_INFINITY : ESTestCase.randomFloatBetween(0.001f, 1000f, true)
        );
        request.setSlices(ESTestCase.between(1, 50));
        request.setShouldStoreResult(ESTestCase.randomBoolean());
        request.setEligibleForRelocationOnShutdown(ESTestCase.randomBoolean());
        if (ESTestCase.randomBoolean()) {
            request.setSourceIndicesForDescription(new String[] { "idx1", "idx2" });
        }
        if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES && request.getMaxDocs() < request.getSlices()) {
            request.setMaxDocs(request.getSlices());
        }
    }

    /**
     * Mutates {@code mutatedRequest} (a copy of {@code originalRequest}) by changing exactly one serialized field of
     * {@link AbstractBulkByScrollRequest}.
     */
    public static void mutateAbstractBulkByScrollRequest(
        AbstractBulkByScrollRequest<?> originalRequest,
        AbstractBulkByScrollRequest<?> mutatedRequest
    ) {
        switch (ESTestCase.between(0, 14)) {
            case 0 -> {
                do {
                    mutatedRequest.getSearchRequest().indices(ESTestCase.randomAlphaOfLength(12));
                } while (Arrays.equals(originalRequest.getSearchRequest().indices(), mutatedRequest.getSearchRequest().indices()));
            }
            case 1 -> mutatedRequest.getSearchRequest()
                .source()
                .size(
                    ESTestCase.randomValueOtherThan(originalRequest.getSearchRequest().source().size(), () -> ESTestCase.between(1, 2000))
                );
            case 2 -> mutatedRequest.setAbortOnVersionConflict(originalRequest.isAbortOnVersionConflict() == false);
            case 3 -> mutatedRequest.setMaxDocs(
                ESTestCase.randomValueOtherThan(originalRequest.getMaxDocs(), () -> ESTestCase.between(10, 50000))
            );
            case 4 -> mutatedRequest.setRefresh(originalRequest.isRefresh() == false);
            case 5 -> mutatedRequest.setTimeout(ESTestCase.randomValueOtherThan(originalRequest.getTimeout(), ESTestCase::randomTimeValue));
            case 6 -> mutatedRequest.setWaitForActiveShards(
                ESTestCase.randomValueOtherThan(
                    originalRequest.getWaitForActiveShards(),
                    () -> ESTestCase.randomFrom(ActiveShardCount.ALL, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.DEFAULT)
                )
            );
            case 7 -> mutatedRequest.setRetryBackoffInitialTime(
                ESTestCase.randomValueOtherThan(originalRequest.getRetryBackoffInitialTime(), ESTestCase::randomPositiveTimeValue)
            );
            case 8 -> mutatedRequest.setMaxRetries(
                ESTestCase.randomValueOtherThan(originalRequest.getMaxRetries(), () -> ESTestCase.between(0, 50))
            );
            case 9 -> mutatedRequest.setRequestsPerSecond(
                ESTestCase.randomValueOtherThan(
                    originalRequest.getRequestsPerSecond(),
                    () -> ESTestCase.randomFloatBetween(0.001f, 2000f, true)
                )
            );
            case 10 -> mutatedRequest.setSlices(
                ESTestCase.randomValueOtherThan(originalRequest.getSlices(), () -> ESTestCase.between(1, 100))
            );
            case 11 -> mutatedRequest.setShouldStoreResult(originalRequest.getShouldStoreResult() == false);
            case 12 -> mutatedRequest.setEligibleForRelocationOnShutdown(originalRequest.isEligibleForRelocationOnShutdown() == false);
            case 13 -> mutatedRequest.setResumeInfo(
                ESTestCase.randomValueOtherThan(
                    originalRequest.getResumeInfo().orElse(null),
                    BulkByScrollWireSerializingTestUtils::randomResumeInfo
                )
            );
            case 14 -> {
                String[] originalIndices = originalRequest.getSourceIndicesForDescription();
                if (originalIndices == null) {
                    mutatedRequest.setSourceIndicesForDescription(new String[] { ESTestCase.randomAlphaOfLength(6) });
                } else {
                    do {
                        mutatedRequest.setSourceIndicesForDescription(
                            new String[] { ESTestCase.randomAlphaOfLength(9), ESTestCase.randomAlphaOfLength(9) }
                        );
                    } while (Arrays.equals(originalIndices, mutatedRequest.getSourceIndicesForDescription()));
                }
            }
            default -> throw new AssertionError();
        }
        if (mutatedRequest.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES
            && mutatedRequest.getMaxDocs() < mutatedRequest.getSlices()) {
            mutatedRequest.setMaxDocs(mutatedRequest.getSlices());
        }
    }

    /**
     * Random {@link ResumeInfo}, either a single worker or multiple slices. All workers in the instance are either
     * scroll-based or PIT-based, not a mix.
     */
    public static ResumeInfo randomResumeInfo() {
        ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(
            new TaskId(ESTestCase.randomAlphaOfLength(8), ESTestCase.randomNonNegativeLong()),
            ESTestCase.randomNonNegativeLong()
        );
        boolean pitWorkers = ESTestCase.randomBoolean();
        if (ESTestCase.randomBoolean()) {
            ResumeInfo.WorkerResumeInfo worker = pitWorkers ? randomPitWorkerResumeInfo() : randomScrollWorkerResumeInfo();
            return new ResumeInfo(origin, worker, null);
        }
        int sliceCount = ESTestCase.randomIntBetween(2, 4);
        HashMap<Integer, ResumeInfo.SliceStatus> slices = new HashMap<>();
        for (int sliceIndex = 0; sliceIndex < sliceCount; sliceIndex++) {
            ResumeInfo.WorkerResumeInfo worker = pitWorkers ? randomPitWorkerResumeInfo() : randomScrollWorkerResumeInfo();
            slices.put(sliceIndex, new ResumeInfo.SliceStatus(sliceIndex, worker, null));
        }
        return new ResumeInfo(origin, null, slices);
    }
}
