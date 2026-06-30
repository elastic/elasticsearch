/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Information returned when one of more {@link Driver}s is completed.
 * @param documentsFound The number of documents found by all lucene queries performed by these drivers.
 * @param valuesLoaded The number of values loaded from lucene for all drivers. This is
 *                     <strong>roughly</strong> the number of documents times the number of
 *                     fields per document. Except {@code null} values don't count.
 *                     And multivalued fields count as many times as there are values.
 * @param rowsEmitted Total rows emitted by source operators across all drivers.
 * @param bytesRead Total bytes read across all drivers. Includes pre-decompression bytes pulled from
 *                  external storage by external-source operators, bytes read by Lucene-source and
 *                  values-source operators on worker threads, and planner-time Lucene directory I/O
 *                  on the data node SEARCH thread (query rewriting, weight construction,
 *                  {@code SearchStats} field lookups, sort builders, etc.).
 *                  TODO: Lookup join streaming reads are not included yet here.
 * @param readNanos Total wall time format readers spent reading on producer threads, in nanoseconds.
 *                  Lucene contributes 0; only external-source operators populate this.
 * @param cpuNanos Total CPU time across all drivers (sum of per-driver CPU time).
 * @param driverProfiles {@link DriverProfile}s from each driver. These are fairly cheap to build but
 *                          not free so this will be empty if the {@code profile} option was not set in
 *                          the request.
 * @param capturedSourceMetadata Per-file flat {@code _stats.*} metadata contributions captured during
 *                               data-node execution and shipped back so the coordinator can merge and
 *                               enrich its {@code SchemaCacheEntry} for the next query. Keyed by file
 *                               path; the value is the list of contributions from each operator/driver
 *                               that touched the file (per-chunk for parallel parsing, per-split for
 *                               macro-splits). The actual merge is intentionally deferred to esql-side
 *                               consumers because the merge algorithm lives in
 *                               {@code SourceStatisticsSerializer.mergeStatistics} which depends on
 *                               esql-module types this compute module cannot reach.
 * @param partial Whether any driver returned partial results because a lenient policy dropped data during
 *                the read (e.g. a {@code max_record_size} truncation under a non-strict {@code error_mode}).
 *                OR-aggregated across drivers/nodes and consumed by the coordinator to flip the response's
 *                {@code is_partial} flag — the structured counterpart of the client-visible truncation warning.
 */
public record DriverCompletionInfo(
    long documentsFound,
    long valuesLoaded,
    long rowsEmitted,
    long bytesRead,
    long readNanos,
    long cpuNanos,
    List<DriverProfile> driverProfiles,
    List<PlanProfile> planProfiles,
    Map<String, List<Map<String, Object>>> capturedSourceMetadata,
    boolean partial
) implements Writeable {

    /**
     * Completion info we use when we didn't properly complete any drivers.
     * Usually this is returned with an error, but it's also used when receiving
     * responses from very old nodes.
     */
    public static final DriverCompletionInfo EMPTY = new DriverCompletionInfo(0, 0, 0, 0, 0, 0, List.of(), List.of(), Map.of(), false);

    public DriverCompletionInfo {
        capturedSourceMetadata = capturedSourceMetadata == null ? Map.of() : capturedSourceMetadata;
    }

    /**
     * Build a {@link DriverCompletionInfo} for many drivers including their profile output.
     *
     * @param planningBytesRead Bytes read on the data node SEARCH thread during planner setup
     *                          (query rewriting, weight construction, {@code SearchStats} lookups,
     *                          sort builders, etc.) before drivers were dispatched. Added to the
     *                          aggregate {@code bytesRead}.
     */
    public static DriverCompletionInfo includingProfiles(
        List<Driver> drivers,
        String description,
        String clusterName,
        String nodeName,
        String planTree,
        String logicalPlanTree,
        PlanTimeProfile planTimeProfile,
        long planningBytesRead
    ) {
        long documentsFound = 0;
        long valuesLoaded = 0;
        long rowsEmitted = 0;
        long bytesRead = planningBytesRead;
        long readNanos = 0;
        long cpuNanos = 0;
        List<DriverProfile> collectedProfiles = new ArrayList<>(drivers.size());
        for (Driver d : drivers) {
            DriverProfile p = d.profile();
            for (OperatorStatus o : p.operators()) {
                documentsFound += o.documentsFound();
                valuesLoaded += o.valuesLoaded();
                rowsEmitted += o.rowsEmitted();
                bytesRead += o.bytesRead();
                readNanos += o.readNanos();
            }
            cpuNanos += p.cpuNanos();
            collectedProfiles.add(p);
        }
        return new DriverCompletionInfo(
            documentsFound,
            valuesLoaded,
            rowsEmitted,
            bytesRead,
            readNanos,
            cpuNanos,
            collectedProfiles,
            List.of(new PlanProfile(description, clusterName, nodeName, planTree, logicalPlanTree, planTimeProfile)),
            collectCapturedSourceMetadata(drivers),
            collectPartial(drivers)
        );
    }

    /**
     * Build a {@link DriverCompletionInfo} for many drivers excluding their profile output.
     *
     * @param planningBytesRead Bytes read on the data node SEARCH thread during planner setup
     *                          (query rewriting, weight construction, {@code SearchStats} lookups,
     *                          sort builders, etc.) before drivers were dispatched. Added to the
     *                          aggregate {@code bytesRead}.
     */
    public static DriverCompletionInfo excludingProfiles(List<Driver> drivers, long planningBytesRead) {
        long documentsFound = 0;
        long valuesLoaded = 0;
        long rowsEmitted = 0;
        long bytesRead = planningBytesRead;
        long readNanos = 0;
        long cpuNanos = 0;
        for (Driver d : drivers) {
            DriverStatus s = d.status();
            assert s.status() == DriverStatus.Status.DONE;
            for (OperatorStatus o : s.completedOperators()) {
                documentsFound += o.documentsFound();
                valuesLoaded += o.valuesLoaded();
                rowsEmitted += o.rowsEmitted();
                bytesRead += o.bytesRead();
                readNanos += o.readNanos();
            }
            cpuNanos += s.cpuNanos();
        }
        return new DriverCompletionInfo(
            documentsFound,
            valuesLoaded,
            rowsEmitted,
            bytesRead,
            readNanos,
            cpuNanos,
            List.of(),
            List.of(),
            collectCapturedSourceMetadata(drivers),
            collectPartial(drivers)
        );
    }

    /**
     * Walks every driver's completed operators, pulls each contribution off any
     * {@link CapturingExternalSourceStatus} the operators expose, and concatenates per file path.
     * The actual per-path merge is deferred to esql-side consumers (see class Javadoc).
     */
    private static Map<String, List<Map<String, Object>>> collectCapturedSourceMetadata(List<Driver> drivers) {
        Map<String, List<Map<String, Object>>> perFile = null;
        for (Driver d : drivers) {
            DriverStatus s = d.status();
            for (OperatorStatus o : s.completedOperators()) {
                Operator.Status status = o.status();
                if (status instanceof CapturingExternalSourceStatus capturing) {
                    Map<String, List<Map<String, Object>>> contribution = capturing.capturedSourceMetadata();
                    if (contribution == null || contribution.isEmpty()) {
                        continue;
                    }
                    if (perFile == null) {
                        perFile = new HashMap<>();
                    }
                    for (Map.Entry<String, List<Map<String, Object>>> e : contribution.entrySet()) {
                        perFile.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
                    }
                }
            }
        }
        return perFile == null ? Map.of() : perFile;
    }

    /**
     * ORs the {@link CapturingExternalSourceStatus#partial()} flag across every completed operator. True when
     * any external-source read on any driver dropped data under a lenient policy (e.g. {@code max_record_size}
     * truncation), so the coordinator can flip the response's {@code is_partial} flag.
     */
    private static boolean collectPartial(List<Driver> drivers) {
        for (Driver d : drivers) {
            for (OperatorStatus o : d.status().completedOperators()) {
                if (o.status() instanceof CapturingExternalSourceStatus capturing && capturing.partial()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final TransportVersion ESQL_PROFILE_INCLUDE_PLAN = TransportVersion.fromName("esql_profile_include_plan");
    private static final TransportVersion ESQL_CAPTURED_SOURCE_METADATA = TransportVersion.fromName("esql_captured_source_metadata");
    // Also gates AsyncExternalSourceOperator.Status fields and EsqlQueryProfile.datasetResolution.
    private static final TransportVersion ESQL_EXTERNAL_SOURCE_PROFILE = TransportVersion.fromName("esql_external_source_profile");
    private static final TransportVersion ESQL_EXTERNAL_PARTIAL_RESULTS = TransportVersion.fromName("esql_external_partial_results");

    public static DriverCompletionInfo readFrom(StreamInput in) throws IOException {
        long documentsFound = in.readVLong();
        long valuesLoaded = in.readVLong();
        long rowsEmitted = 0;
        long bytesRead = 0;
        long readNanos = 0;
        long cpuNanos = 0;
        if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
            rowsEmitted = in.readVLong();
            bytesRead = in.readVLong();
            readNanos = in.readVLong();
            cpuNanos = in.readVLong();
        }
        List<DriverProfile> driverProfiles = in.readCollectionAsImmutableList(DriverProfile::readFrom);
        List<PlanProfile> planProfiles = in.getTransportVersion().supports(ESQL_PROFILE_INCLUDE_PLAN)
            ? in.readCollectionAsImmutableList(PlanProfile::readFrom)
            : List.of();
        Map<String, List<Map<String, Object>>> captured;
        if (in.getTransportVersion().supports(ESQL_CAPTURED_SOURCE_METADATA)) {
            int n = in.readVInt();
            if (n == 0) {
                captured = Map.of();
            } else {
                Map<String, List<Map<String, Object>>> tmp = new HashMap<>(n);
                for (int i = 0; i < n; i++) {
                    String path = in.readString();
                    int contributionCount = in.readVInt();
                    List<Map<String, Object>> contributions = new ArrayList<>(contributionCount);
                    for (int j = 0; j < contributionCount; j++) {
                        contributions.add(in.readGenericMap());
                    }
                    tmp.put(path, contributions);
                }
                captured = tmp;
            }
        } else {
            captured = Map.of();
        }
        boolean partial = in.getTransportVersion().supports(ESQL_EXTERNAL_PARTIAL_RESULTS) && in.readBoolean();
        return new DriverCompletionInfo(
            documentsFound,
            valuesLoaded,
            rowsEmitted,
            bytesRead,
            readNanos,
            cpuNanos,
            driverProfiles,
            planProfiles,
            captured,
            partial
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(documentsFound);
        out.writeVLong(valuesLoaded);
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
            out.writeVLong(rowsEmitted);
            out.writeVLong(bytesRead);
            out.writeVLong(readNanos);
            out.writeVLong(cpuNanos);
        }
        out.writeCollection(driverProfiles);
        if (out.getTransportVersion().supports(ESQL_PROFILE_INCLUDE_PLAN)) {
            out.writeCollection(planProfiles);
        }
        if (out.getTransportVersion().supports(ESQL_CAPTURED_SOURCE_METADATA)) {
            out.writeVInt(capturedSourceMetadata.size());
            for (Map.Entry<String, List<Map<String, Object>>> e : capturedSourceMetadata.entrySet()) {
                out.writeString(e.getKey());
                List<Map<String, Object>> contributions = e.getValue();
                out.writeVInt(contributions.size());
                for (Map<String, Object> contribution : contributions) {
                    out.writeGenericMap(contribution);
                }
            }
        }
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_PARTIAL_RESULTS)) {
            out.writeBoolean(partial);
        }
    }

    public static class Accumulator {
        private long documentsFound;
        private long valuesLoaded;
        private long rowsEmitted;
        private long bytesRead;
        private long readNanos;
        private long cpuNanos;
        private final List<DriverProfile> driverProfiles = new ArrayList<>();
        private final List<PlanProfile> planProfiles = new ArrayList<>();
        private final Map<String, List<Map<String, Object>>> capturedSourceMetadata = new HashMap<>();
        private boolean partial;

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound += info.documentsFound;
            this.valuesLoaded += info.valuesLoaded;
            this.rowsEmitted += info.rowsEmitted;
            this.bytesRead += info.bytesRead;
            this.readNanos += info.readNanos;
            this.cpuNanos += info.cpuNanos;
            this.driverProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
            mergeCapturedSourceMetadata(capturedSourceMetadata, info.capturedSourceMetadata);
            this.partial |= info.partial;
        }

        public DriverCompletionInfo finish() {
            return new DriverCompletionInfo(
                documentsFound,
                valuesLoaded,
                rowsEmitted,
                bytesRead,
                readNanos,
                cpuNanos,
                driverProfiles,
                planProfiles,
                capturedSourceMetadata.isEmpty() ? Map.of() : new HashMap<>(capturedSourceMetadata),
                partial
            );
        }
    }

    public static class AtomicAccumulator {
        private final AtomicLong documentsFound = new AtomicLong();
        private final AtomicLong valuesLoaded = new AtomicLong();
        private final AtomicLong rowsEmitted = new AtomicLong();
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicLong readNanos = new AtomicLong();
        private final AtomicLong cpuNanos = new AtomicLong();
        private final List<DriverProfile> collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        private final List<PlanProfile> planProfiles = Collections.synchronizedList(new ArrayList<>());
        private final Map<String, List<Map<String, Object>>> capturedSourceMetadata = new HashMap<>();
        private final AtomicBoolean partial = new AtomicBoolean();

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound.addAndGet(info.documentsFound);
            this.valuesLoaded.addAndGet(info.valuesLoaded);
            this.rowsEmitted.addAndGet(info.rowsEmitted);
            this.bytesRead.addAndGet(info.bytesRead);
            this.readNanos.addAndGet(info.readNanos);
            this.cpuNanos.addAndGet(info.cpuNanos);
            this.collectedProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
            synchronized (capturedSourceMetadata) {
                mergeCapturedSourceMetadata(capturedSourceMetadata, info.capturedSourceMetadata);
            }
            if (info.partial) {
                this.partial.set(true);
            }
        }

        public DriverCompletionInfo finish() {
            Map<String, List<Map<String, Object>>> snapshot;
            synchronized (capturedSourceMetadata) {
                snapshot = capturedSourceMetadata.isEmpty() ? Map.of() : new HashMap<>(capturedSourceMetadata);
            }
            return new DriverCompletionInfo(
                documentsFound.get(),
                valuesLoaded.get(),
                rowsEmitted.get(),
                bytesRead.get(),
                readNanos.get(),
                cpuNanos.get(),
                collectedProfiles,
                planProfiles,
                snapshot,
                partial.get()
            );
        }
    }

    /**
     * Concatenates {@code incoming}'s per-path contribution lists into {@code into}. The actual
     * per-path merge of the resulting flat-map contributions is deferred to esql-side consumers.
     */
    private static void mergeCapturedSourceMetadata(
        Map<String, List<Map<String, Object>>> into,
        Map<String, List<Map<String, Object>>> incoming
    ) {
        if (incoming == null || incoming.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<Map<String, Object>>> e : incoming.entrySet()) {
            into.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
        }
    }
}
