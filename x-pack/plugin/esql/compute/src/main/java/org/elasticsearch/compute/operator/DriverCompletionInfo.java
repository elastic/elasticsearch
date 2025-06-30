/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersions;
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
 * @param driverProfiles {@link DriverProfile}s from each driver. These are fairly cheap to build but
 *                          not free so this will be empty if the {@code profile} option was not set in
 *                          the request.
 */
public record DriverCompletionInfo(
    long documentsFound,
    long valuesLoaded,
    List<DriverProfile> driverProfiles,
    List<PlanProfile> planProfiles
) implements Writeable {

    /**
     * Completion info we use when we didn't properly complete any drivers.
     * Usually this is returned with an error, but it's also used when receiving
     * responses from very old nodes.
     */
    public static final DriverCompletionInfo EMPTY = new DriverCompletionInfo(0, 0, List.of(), List.of());

    /**
     * Build a {@link DriverCompletionInfo} for many drivers including their profile output.
     */
    public static DriverCompletionInfo includingProfiles(
        List<Driver> drivers,
        String description,
        String clusterName,
        String nodeName,
        String planTree
    ) {
        long documentsFound = 0;
        long valuesLoaded = 0;
        List<DriverProfile> collectedProfiles = new ArrayList<>(drivers.size());
        for (Driver d : drivers) {
            DriverProfile p = d.profile();
            for (OperatorStatus o : p.operators()) {
                documentsFound += o.documentsFound();
                valuesLoaded += o.valuesLoaded();
            }
            collectedProfiles.add(p);
        }
        return new DriverCompletionInfo(
            documentsFound,
            valuesLoaded,
            collectedProfiles,
            List.of(new PlanProfile(description, clusterName, nodeName, planTree))
        );
    }

    /**
     * Build a {@link DriverCompletionInfo} for many drivers excluding their profile output.
     */
    public static DriverCompletionInfo excludingProfiles(List<Driver> drivers) {
        long documentsFound = 0;
        long valuesLoaded = 0;
        for (Driver d : drivers) {
            DriverStatus s = d.status();
            assert s.status() == DriverStatus.Status.DONE;
            for (OperatorStatus o : s.completedOperators()) {
                documentsFound += o.documentsFound();
                valuesLoaded += o.valuesLoaded();
            }
        }
        return new DriverCompletionInfo(documentsFound, valuesLoaded, List.of(), List.of());
    }

    public static DriverCompletionInfo readFrom(StreamInput in) throws IOException {
        return new DriverCompletionInfo(
            in.readVLong(),
            in.readVLong(),
            in.readCollectionAsImmutableList(DriverProfile::readFrom),
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_INCLUDE_PLAN)
                || in.getTransportVersion().isPatchFrom(TransportVersions.ESQL_PROFILE_INCLUDE_PLAN_8_19)
                    ? in.readCollectionAsImmutableList(PlanProfile::readFrom)
                    : List.of()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(documentsFound);
        out.writeVLong(valuesLoaded);
        out.writeCollection(driverProfiles);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_INCLUDE_PLAN)
            || out.getTransportVersion().isPatchFrom(TransportVersions.ESQL_PROFILE_INCLUDE_PLAN_8_19)) {
            out.writeCollection(planProfiles);
        }
    }

    public static class Accumulator {
        private long documentsFound;
        private long valuesLoaded;
        private final List<DriverProfile> driverProfiles = new ArrayList<>();
        private final List<PlanProfile> planProfiles = new ArrayList<>();

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound += info.documentsFound;
            this.valuesLoaded += info.valuesLoaded;
            this.driverProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
        }

        public DriverCompletionInfo finish() {
            return new DriverCompletionInfo(documentsFound, valuesLoaded, driverProfiles, planProfiles);
        }
    }

    public static class AtomicAccumulator {
        private final AtomicLong documentsFound = new AtomicLong();
        private final AtomicLong valuesLoaded = new AtomicLong();
        private final List<DriverProfile> collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        private final List<PlanProfile> planProfiles = Collections.synchronizedList(new ArrayList<>());

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound.addAndGet(info.documentsFound);
            this.valuesLoaded.addAndGet(info.valuesLoaded);
            this.collectedProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
        }

        public DriverCompletionInfo finish() {
            return new DriverCompletionInfo(documentsFound.get(), valuesLoaded.get(), collectedProfiles, planProfiles);
        }
    }
}
