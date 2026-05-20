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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Information returned when one of more {@link Driver}s is completed.
 * @param documentsFound The number of documents found by all lucene queries performed by these drivers.
 * @param valuesLoaded The number of values loaded from lucene for all drivers. This is
 *                     <strong>roughly</strong> the number of documents times the number of
 *                     fields per document. Except {@code null} values don't count.
 *                     And multivalued fields count as many times as there are values.
 * @param rowsEmitted Total rows emitted by source operators across all drivers.
 * @param bytesRead Total pre-decompression bytes pulled from external storage across all drivers.
 *                  Lucene operators contribute 0; only external-source operators populate this.
 * @param readNanos Total wall time format readers spent reading on producer threads, in nanoseconds.
 *                  Lucene contributes 0; only external-source operators populate this.
 * @param cpuNanos Total CPU time across all drivers (sum of per-driver CPU time).
 * @param driverProfiles {@link DriverProfile}s from each driver. These are fairly cheap to build but
 *                          not free so this will be empty if the {@code profile} option was not set in
 *                          the request.
 */
public record DriverCompletionInfo(
    long documentsFound,
    long valuesLoaded,
    long rowsEmitted,
    long bytesRead,
    long readNanos,
    long cpuNanos,
    List<DriverProfile> driverProfiles,
    List<PlanProfile> planProfiles
) implements Writeable {

    /**
     * Completion info we use when we didn't properly complete any drivers.
     * Usually this is returned with an error, but it's also used when receiving
     * responses from very old nodes.
     */
    public static final DriverCompletionInfo EMPTY = new DriverCompletionInfo(0, 0, 0, 0, 0, 0, List.of(), List.of());

    /**
     * Build a {@link DriverCompletionInfo} for many drivers including their profile output.
     */
    public static DriverCompletionInfo includingProfiles(
        List<Driver> drivers,
        String description,
        String clusterName,
        String nodeName,
        String planTree,
        String logicalPlanTree,
        PlanTimeProfile planTimeProfile
    ) {
        long documentsFound = 0;
        long valuesLoaded = 0;
        long rowsEmitted = 0;
        long bytesRead = 0;
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
            List.of(new PlanProfile(description, clusterName, nodeName, planTree, logicalPlanTree, planTimeProfile))
        );
    }

    /**
     * Build a {@link DriverCompletionInfo} for many drivers excluding their profile output.
     */
    public static DriverCompletionInfo excludingProfiles(List<Driver> drivers) {
        long documentsFound = 0;
        long valuesLoaded = 0;
        long rowsEmitted = 0;
        long bytesRead = 0;
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
        return new DriverCompletionInfo(documentsFound, valuesLoaded, rowsEmitted, bytesRead, readNanos, cpuNanos, List.of(), List.of());
    }

    private static final TransportVersion ESQL_PROFILE_INCLUDE_PLAN = TransportVersion.fromName("esql_profile_include_plan");
    // Also gates AsyncExternalSourceOperator.Status fields and EsqlQueryProfile.datasetResolution.
    private static final TransportVersion ESQL_EXTERNAL_SOURCE_PROFILE = TransportVersion.fromName("esql_external_source_profile");

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
        return new DriverCompletionInfo(
            documentsFound,
            valuesLoaded,
            rowsEmitted,
            bytesRead,
            readNanos,
            cpuNanos,
            driverProfiles,
            planProfiles
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

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound += info.documentsFound;
            this.valuesLoaded += info.valuesLoaded;
            this.rowsEmitted += info.rowsEmitted;
            this.bytesRead += info.bytesRead;
            this.readNanos += info.readNanos;
            this.cpuNanos += info.cpuNanos;
            this.driverProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
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
                planProfiles
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

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound.addAndGet(info.documentsFound);
            this.valuesLoaded.addAndGet(info.valuesLoaded);
            this.rowsEmitted.addAndGet(info.rowsEmitted);
            this.bytesRead.addAndGet(info.bytesRead);
            this.readNanos.addAndGet(info.readNanos);
            this.cpuNanos.addAndGet(info.cpuNanos);
            this.collectedProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
        }

        public DriverCompletionInfo finish() {
            return new DriverCompletionInfo(
                documentsFound.get(),
                valuesLoaded.get(),
                rowsEmitted.get(),
                bytesRead.get(),
                readNanos.get(),
                cpuNanos.get(),
                collectedProfiles,
                planProfiles
            );
        }
    }
}
