/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class CrossProjectSearchMetrics implements ToXContentFragment {
    private long preProcessingTookTime;
    private long planningPhaseTookTime;
    private long mergingPhaseTookTime;
    /*
     * Tracks the time taken from dispatching a request to a linked project to receiving a response (meaning, also includes the
     * network time).
     */
    private final Map<String, Long> perProjectRoundtripTime;
    /**
     * Coordinator-observed wall-clock duration (ms) for each search phase (e.g. "query", "fetch", "dfs").
     * Populated only when this request resolves as a Cross-Project Search.
     */
    private final Map<String, Long> searchPhaseTookTimes;
    private final Map<String, FetchProjectDiagnostics> fetchPhaseDiagnosticsByProject;

    public static final String CPS_PROFILE_FIELD = "cps_profile";
    public static final ParseField PRE_PROCESSING_TOOK_TIME_FIELD = new ParseField("preprocessing_took_time");
    public static final ParseField PLANNING_PHASE_TOOK_TIME_FIELD = new ParseField("planning_phase_took_time");
    public static final ParseField MERGING_PHASE_TOOK_TIME_FIELD = new ParseField("merging_phase_took_time");
    public static final String PROJECTS_ROUND_TRIP_TIME = "projects_round_trip_time";
    public static final String PROJECTS_NAME = "projects";
    public static final String SEARCH_PHASES_FIELD = "search_phases";
    public static final String FETCH_PHASE_DIAGNOSTICS_FIELD = "fetch_phase_diagnostics";
    public static final String PROJECT_FIELD = "project";
    public static final String FETCH_SHARD_COUNT_FIELD = "fetch_shard_count";
    public static final String FETCH_RTT_FULL_MS_FIELD = "fetch_rtt_full_ms";
    public static final String FETCH_QUEUE_WAIT_MS_FIELD = "fetch_queue_wait_ms";
    public static final String FETCH_SERVICE_MS_FIELD = "fetch_service_ms";
    public static final String NETWORK_PLUS_SERIALIZE_DECODE_MS_FIELD = "network_plus_serialize_decode_ms";
    public static final String FETCH_RESPONSE_BYTES_UNCOMPRESSED_FIELD = "fetch_response_bytes_uncompressed";

    public CrossProjectSearchMetrics() {
        this.preProcessingTookTime = 0;
        this.planningPhaseTookTime = 0L;
        this.mergingPhaseTookTime = 0L;
        this.perProjectRoundtripTime = new HashMap<>();
        this.searchPhaseTookTimes = new LinkedHashMap<>();
        this.fetchPhaseDiagnosticsByProject = new ConcurrentHashMap<>();
    }

    public void trackPreProcessingTookTime(long time) {
        this.preProcessingTookTime = time;
    }

    public void trackPlanningPhaseTookTime(long planningPhaseTookTime) {
        this.planningPhaseTookTime = planningPhaseTookTime;
    }

    public void trackProjectRoundtripTime(String projectName, long projectTookTime) {
        if (projectName.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            projectName = "_origin";
        }

        this.perProjectRoundtripTime.put(projectName, projectTookTime);
    }

    public void trackMergingPhaseTookTime(long mergingPhaseTookTime) {
        this.mergingPhaseTookTime = mergingPhaseTookTime;
    }

    /**
     * Records the coordinator-observed wall-clock duration for a named search phase (e.g. "query", "fetch", "dfs").
     * Subsequent calls for the same phase name overwrite the previous value.
     */
    public void trackSearchPhaseTookTime(String phaseName, long tookMs) {
        this.searchPhaseTookTimes.put(phaseName, tookMs);
    }

    public Map<String, Long> getSearchPhaseTookTimes() {
        return searchPhaseTookTimes;
    }

    public void trackProjectFetchDiagnostics(
        String projectName,
        long fetchRttFullMs,
        long fetchQueueWaitMs,
        long fetchServiceMs,
        long responseBytesUncompressed
    ) {
        if (projectName == null || projectName.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            projectName = "_origin";
        }
        long networkPlusSerializeDecodeMs = -1L;
        if (fetchRttFullMs >= 0L && fetchQueueWaitMs >= 0L && fetchServiceMs >= 0L) {
            networkPlusSerializeDecodeMs = Math.max(0L, fetchRttFullMs - fetchQueueWaitMs - fetchServiceMs);
        }
        fetchPhaseDiagnosticsByProject.computeIfAbsent(projectName, k -> new FetchProjectDiagnostics())
            .add(fetchRttFullMs, fetchQueueWaitMs, fetchServiceMs, networkPlusSerializeDecodeMs, responseBytesUncompressed);
    }

    public Map<String, Map<String, Long>> getFetchPhaseDiagnosticsByProject() {
        Map<String, Map<String, Long>> snapshot = new HashMap<>();
        fetchPhaseDiagnosticsByProject.forEach((project, diagnostics) -> snapshot.put(project, diagnostics.asMap()));
        return snapshot;
    }

    public long getPlanningPhaseTookTime() {
        return planningPhaseTookTime;
    }

    public Map<String, Long> getProjectsRoundtripTime() {
        return perProjectRoundtripTime;
    }

    public long getMergingPhaseTookTime() {
        return mergingPhaseTookTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(CPS_PROFILE_FIELD);

        builder.field(PRE_PROCESSING_TOOK_TIME_FIELD.getPreferredName(), preProcessingTookTime);
        builder.field(PLANNING_PHASE_TOOK_TIME_FIELD.getPreferredName(), planningPhaseTookTime);
        builder.field(MERGING_PHASE_TOOK_TIME_FIELD.getPreferredName(), mergingPhaseTookTime);

        builder.startObject(PROJECTS_ROUND_TRIP_TIME).startArray(PROJECTS_NAME);

        TreeSet<String> sorted = new TreeSet<>(perProjectRoundtripTime.keySet());
        for (String projectName : sorted) {
            long projectTookTime = perProjectRoundtripTime.get(projectName);

            builder.startObject();
            builder.field(projectName, projectTookTime);
            builder.endObject();
        }

        builder.endArray().endObject();

        if (searchPhaseTookTimes.isEmpty() == false) {
            builder.startObject(SEARCH_PHASES_FIELD);
            for (Map.Entry<String, Long> entry : searchPhaseTookTimes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (fetchPhaseDiagnosticsByProject.isEmpty() == false) {
            builder.startObject(FETCH_PHASE_DIAGNOSTICS_FIELD).startArray(PROJECTS_NAME);
            TreeSet<String> sortedProjects = new TreeSet<>(fetchPhaseDiagnosticsByProject.keySet());
            for (String projectName : sortedProjects) {
                builder.startObject();
                builder.field(PROJECT_FIELD, projectName);
                fetchPhaseDiagnosticsByProject.get(projectName).toXContent(builder);
                builder.endObject();
            }
            builder.endArray().endObject();
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CrossProjectSearchMetrics other
            && other.preProcessingTookTime == this.preProcessingTookTime
            && other.planningPhaseTookTime == this.planningPhaseTookTime
            && other.mergingPhaseTookTime == this.mergingPhaseTookTime
            && other.perProjectRoundtripTime.equals(this.perProjectRoundtripTime)
            && other.searchPhaseTookTimes.equals(this.searchPhaseTookTimes)
            && other.fetchPhaseDiagnosticsByProject.equals(this.fetchPhaseDiagnosticsByProject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            preProcessingTookTime,
            planningPhaseTookTime,
            mergingPhaseTookTime,
            perProjectRoundtripTime,
            searchPhaseTookTimes,
            fetchPhaseDiagnosticsByProject
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Parses a {@code CrossProjectSearchMetrics} from XContent, assuming the parser is positioned
     * just after the {@code cps_profile} field name (i.e. at the start of the inner object).
     */
    public static CrossProjectSearchMetrics fromXContent(XContentParser parser) throws IOException {
        CrossProjectSearchMetrics metrics = new CrossProjectSearchMetrics();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (PRE_PROCESSING_TOOK_TIME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    metrics.preProcessingTookTime = parser.longValue();
                } else if (PLANNING_PHASE_TOOK_TIME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    metrics.planningPhaseTookTime = parser.longValue();
                } else if (MERGING_PHASE_TOOK_TIME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    metrics.mergingPhaseTookTime = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (PROJECTS_ROUND_TRIP_TIME.equals(currentFieldName)) {
                    // parse { "projects": [ { "alias": ms }, ... ] }
                    String innerField = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            innerField = parser.currentName();
                        } else if (token == XContentParser.Token.START_ARRAY && PROJECTS_NAME.equals(innerField)) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                String projectAlias = null;
                                Long projectTime = null;
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        projectAlias = parser.currentName();
                                    } else if (token.isValue()) {
                                        projectTime = parser.longValue();
                                    }
                                }
                                if (projectAlias != null && projectTime != null) {
                                    metrics.perProjectRoundtripTime.put(projectAlias, projectTime);
                                }
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (SEARCH_PHASES_FIELD.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String phaseName = parser.currentName();
                            parser.nextToken();
                            metrics.searchPhaseTookTimes.put(phaseName, parser.longValue());
                        }
                    }
                } else if (FETCH_PHASE_DIAGNOSTICS_FIELD.equals(currentFieldName)) {
                    String diagnosticsInnerField = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            diagnosticsInnerField = parser.currentName();
                        } else if (token == XContentParser.Token.START_ARRAY && PROJECTS_NAME.equals(diagnosticsInnerField)) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                String project = null;
                                long fetchShardCount = 0L;
                                long fetchRttFullMs = -1L;
                                long fetchQueueWaitMs = -1L;
                                long fetchServiceMs = -1L;
                                long networkPlusSerializeDecodeMs = -1L;
                                long fetchResponseBytesUncompressed = -1L;
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        currentFieldName = parser.currentName();
                                    } else if (token.isValue()) {
                                        if (PROJECT_FIELD.equals(currentFieldName)) {
                                            project = parser.text();
                                        } else if (FETCH_SHARD_COUNT_FIELD.equals(currentFieldName)) {
                                            fetchShardCount = parser.longValue();
                                        } else if (FETCH_RTT_FULL_MS_FIELD.equals(currentFieldName)) {
                                            fetchRttFullMs = parser.longValue();
                                        } else if (FETCH_QUEUE_WAIT_MS_FIELD.equals(currentFieldName)) {
                                            fetchQueueWaitMs = parser.longValue();
                                        } else if (FETCH_SERVICE_MS_FIELD.equals(currentFieldName)) {
                                            fetchServiceMs = parser.longValue();
                                        } else if (NETWORK_PLUS_SERIALIZE_DECODE_MS_FIELD.equals(currentFieldName)) {
                                            networkPlusSerializeDecodeMs = parser.longValue();
                                        } else if (FETCH_RESPONSE_BYTES_UNCOMPRESSED_FIELD.equals(currentFieldName)) {
                                            fetchResponseBytesUncompressed = parser.longValue();
                                        } else {
                                            parser.skipChildren();
                                        }
                                    } else {
                                        parser.skipChildren();
                                    }
                                }
                                if (project != null) {
                                    FetchProjectDiagnostics diagnostics = new FetchProjectDiagnostics();
                                    diagnostics.setAveragesFromParsedValues(
                                        fetchShardCount,
                                        fetchRttFullMs,
                                        fetchQueueWaitMs,
                                        fetchServiceMs,
                                        networkPlusSerializeDecodeMs,
                                        fetchResponseBytesUncompressed
                                    );
                                    metrics.fetchPhaseDiagnosticsByProject.put(project, diagnostics);
                                }
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return metrics;
    }

    private static final class FetchProjectDiagnostics {
        private long fetchShardCount;
        private long fetchRttFullSamples;
        private long fetchRttFullMsSum;
        private long fetchQueueWaitMsSum;
        private long fetchQueueWaitSamples;
        private long fetchServiceMsSum;
        private long fetchServiceSamples;
        private long networkPlusSerializeDecodeMsSum;
        private long networkPlusSerializeDecodeSamples;
        private long fetchResponseBytesUncompressedSum;
        private long fetchResponseBytesUncompressedSamples;

        synchronized void add(
            long fetchRttFullMs,
            long fetchQueueWaitMs,
            long fetchServiceMs,
            long networkPlusSerializeDecodeMs,
            long responseBytesUncompressed
        ) {
            fetchShardCount++;
            if (fetchRttFullMs >= 0L) {
                fetchRttFullSamples++;
                fetchRttFullMsSum += fetchRttFullMs;
            }
            if (fetchQueueWaitMs >= 0L) {
                fetchQueueWaitMsSum += fetchQueueWaitMs;
                fetchQueueWaitSamples++;
            }
            if (fetchServiceMs >= 0L) {
                fetchServiceMsSum += fetchServiceMs;
                fetchServiceSamples++;
            }
            if (networkPlusSerializeDecodeMs >= 0L) {
                networkPlusSerializeDecodeMsSum += networkPlusSerializeDecodeMs;
                networkPlusSerializeDecodeSamples++;
            }
            if (responseBytesUncompressed >= 0L) {
                fetchResponseBytesUncompressedSum += responseBytesUncompressed;
                fetchResponseBytesUncompressedSamples++;
            }
        }

        synchronized void toXContent(XContentBuilder builder) throws IOException {
            builder.field(FETCH_SHARD_COUNT_FIELD, fetchShardCount);
            builder.field(FETCH_RTT_FULL_MS_FIELD, averageOrUnknown(fetchRttFullMsSum, fetchRttFullSamples));
            builder.field(FETCH_QUEUE_WAIT_MS_FIELD, averageOrUnknown(fetchQueueWaitMsSum, fetchQueueWaitSamples));
            builder.field(FETCH_SERVICE_MS_FIELD, averageOrUnknown(fetchServiceMsSum, fetchServiceSamples));
            builder.field(
                NETWORK_PLUS_SERIALIZE_DECODE_MS_FIELD,
                averageOrUnknown(networkPlusSerializeDecodeMsSum, networkPlusSerializeDecodeSamples)
            );
            builder.field(
                FETCH_RESPONSE_BYTES_UNCOMPRESSED_FIELD,
                averageOrUnknown(fetchResponseBytesUncompressedSum, fetchResponseBytesUncompressedSamples)
            );
        }

        synchronized Map<String, Long> asMap() {
            Map<String, Long> diagnostics = new LinkedHashMap<>();
            diagnostics.put(FETCH_SHARD_COUNT_FIELD, fetchShardCount);
            diagnostics.put(FETCH_RTT_FULL_MS_FIELD, averageOrUnknown(fetchRttFullMsSum, fetchRttFullSamples));
            diagnostics.put(FETCH_QUEUE_WAIT_MS_FIELD, averageOrUnknown(fetchQueueWaitMsSum, fetchQueueWaitSamples));
            diagnostics.put(FETCH_SERVICE_MS_FIELD, averageOrUnknown(fetchServiceMsSum, fetchServiceSamples));
            diagnostics.put(
                NETWORK_PLUS_SERIALIZE_DECODE_MS_FIELD,
                averageOrUnknown(networkPlusSerializeDecodeMsSum, networkPlusSerializeDecodeSamples)
            );
            diagnostics.put(
                FETCH_RESPONSE_BYTES_UNCOMPRESSED_FIELD,
                averageOrUnknown(fetchResponseBytesUncompressedSum, fetchResponseBytesUncompressedSamples)
            );
            return diagnostics;
        }

        synchronized void setAveragesFromParsedValues(
            long shardCount,
            long fetchRttFullMs,
            long fetchQueueWaitMs,
            long fetchServiceMs,
            long networkPlusSerializeDecodeMs,
            long fetchResponseBytesUncompressed
        ) {
            this.fetchShardCount = shardCount;
            this.fetchRttFullSamples = shardCount > 0L && fetchRttFullMs >= 0L ? shardCount : 0L;
            this.fetchRttFullMsSum = fetchRttFullSamples > 0L ? fetchRttFullMs * fetchRttFullSamples : 0L;
            this.fetchQueueWaitSamples = shardCount > 0L && fetchQueueWaitMs >= 0L ? shardCount : 0L;
            this.fetchQueueWaitMsSum = fetchQueueWaitSamples > 0L ? fetchQueueWaitMs * fetchQueueWaitSamples : 0L;
            this.fetchServiceSamples = shardCount > 0L && fetchServiceMs >= 0L ? shardCount : 0L;
            this.fetchServiceMsSum = fetchServiceSamples > 0L ? fetchServiceMs * fetchServiceSamples : 0L;
            this.networkPlusSerializeDecodeSamples = shardCount > 0L && networkPlusSerializeDecodeMs >= 0L ? shardCount : 0L;
            this.networkPlusSerializeDecodeMsSum = networkPlusSerializeDecodeSamples > 0L
                ? networkPlusSerializeDecodeMs * networkPlusSerializeDecodeSamples
                : 0L;
            this.fetchResponseBytesUncompressedSamples = shardCount > 0L && fetchResponseBytesUncompressed >= 0L ? shardCount : 0L;
            this.fetchResponseBytesUncompressedSum = fetchResponseBytesUncompressedSamples > 0L
                ? fetchResponseBytesUncompressed * fetchResponseBytesUncompressedSamples
                : 0L;
        }

        private static long averageOrUnknown(long sum, long samples) {
            return samples > 0L ? sum / samples : -1L;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof FetchProjectDiagnostics other
                && fetchShardCount == other.fetchShardCount
                && fetchRttFullSamples == other.fetchRttFullSamples
                && fetchRttFullMsSum == other.fetchRttFullMsSum
                && fetchQueueWaitMsSum == other.fetchQueueWaitMsSum
                && fetchQueueWaitSamples == other.fetchQueueWaitSamples
                && fetchServiceMsSum == other.fetchServiceMsSum
                && fetchServiceSamples == other.fetchServiceSamples
                && networkPlusSerializeDecodeMsSum == other.networkPlusSerializeDecodeMsSum
                && networkPlusSerializeDecodeSamples == other.networkPlusSerializeDecodeSamples
                && fetchResponseBytesUncompressedSum == other.fetchResponseBytesUncompressedSum
                && fetchResponseBytesUncompressedSamples == other.fetchResponseBytesUncompressedSamples;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                fetchShardCount,
                fetchRttFullSamples,
                fetchRttFullMsSum,
                fetchQueueWaitMsSum,
                fetchQueueWaitSamples,
                fetchServiceMsSum,
                fetchServiceSamples,
                networkPlusSerializeDecodeMsSum,
                networkPlusSerializeDecodeSamples,
                fetchResponseBytesUncompressedSum,
                fetchResponseBytesUncompressedSamples
            );
        }
    }
}
