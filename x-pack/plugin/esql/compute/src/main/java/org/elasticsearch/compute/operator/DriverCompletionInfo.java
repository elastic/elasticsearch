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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
 * @param warnings Fully-formatted warning strings accumulated per driver into each {@link DriverContext}'s sink
 *                 during execution. Deduplicated across drivers: each unique warning string appears at most once.
 */
public record DriverCompletionInfo(
    long documentsFound,
    long valuesLoaded,
    List<DriverProfile> driverProfiles,
    List<PlanProfile> planProfiles,
    Set<String> warnings
) implements Writeable {

    /**
     * Completion info we use when we didn't properly complete any drivers.
     * Usually this is returned with an error, but it's also used when receiving
     * responses from very old nodes.
     */
    public static final DriverCompletionInfo EMPTY = new DriverCompletionInfo(0, 0, List.of(), List.of(), Set.of());

    public DriverCompletionInfo {
        warnings = warnings == null ? Set.of() : warnings;
    }

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
            List.of(new PlanProfile(description, clusterName, nodeName, planTree, logicalPlanTree, planTimeProfile)),
            collectWarnings(drivers)
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
        return new DriverCompletionInfo(documentsFound, valuesLoaded, List.of(), List.of(), collectWarnings(drivers));
    }

    /**
     * Merge per-driver warnings (see {@link DriverContext#warnings()}) across many drivers,
     * deduplicating in insertion order.
     */
    private static Set<String> collectWarnings(List<Driver> drivers) {
        LinkedHashSet<String> warnings = null;
        for (Driver d : drivers) {
            List<String> driverWarnings = d.driverContext().warnings();
            if (driverWarnings == null || driverWarnings.isEmpty()) {
                continue;
            }
            if (warnings == null) {
                warnings = new LinkedHashSet<>();
            }
            warnings.addAll(driverWarnings);
        }
        return warnings == null ? Set.of() : Collections.unmodifiableSet(warnings);
    }

    private static final TransportVersion ESQL_PROFILE_INCLUDE_PLAN = TransportVersion.fromName("esql_profile_include_plan");
    public static final TransportVersion ESQL_DRIVER_WARNINGS = TransportVersion.fromName("esql_driver_warnings");

    public static DriverCompletionInfo readFrom(StreamInput in, ThreadContext threadContext) throws IOException {
        long documentsFound = in.readVLong();
        long valuesLoaded = in.readVLong();
        List<DriverProfile> driverProfiles = in.readCollectionAsImmutableList(DriverProfile::readFrom);
        List<PlanProfile> planProfiles = in.getTransportVersion().supports(ESQL_PROFILE_INCLUDE_PLAN)
            ? in.readCollectionAsImmutableList(PlanProfile::readFrom)
            : List.of();
        Set<String> warnings;
        if (in.getTransportVersion().supports(ESQL_DRIVER_WARNINGS)) {
            warnings = Collections.unmodifiableSet(in.readCollection(LinkedHashSet::new, (stream, set) -> set.add(stream.readString())));
        } else {
            List<String> headerWarnings = threadContext.takeResponseHeaders("Warning");
            if (headerWarnings.isEmpty()) {
                warnings = Set.of();
            } else {
                LinkedHashSet<String> parsed = new LinkedHashSet<>(headerWarnings.size());
                for (String header : headerWarnings) {
                    String extracted = HeaderWarning.extractWarningValueFromWarningHeader(header, false);
                    parsed.add(HeaderWarning.decodeAndUnescape(extracted));
                }
                warnings = parsed;
            }
        }
        return new DriverCompletionInfo(documentsFound, valuesLoaded, driverProfiles, planProfiles, warnings);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(documentsFound);
        out.writeVLong(valuesLoaded);
        out.writeCollection(driverProfiles);
        if (out.getTransportVersion().supports(ESQL_PROFILE_INCLUDE_PLAN)) {
            out.writeCollection(planProfiles);
        }
        if (out.getTransportVersion().supports(ESQL_DRIVER_WARNINGS)) {
            out.writeStringCollection(warnings);
        }
    }

    public static class Accumulator {
        private long documentsFound;
        private long valuesLoaded;
        private final List<DriverProfile> driverProfiles = new ArrayList<>();
        private final List<PlanProfile> planProfiles = new ArrayList<>();
        private final Set<String> warnings = new LinkedHashSet<>();

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound += info.documentsFound;
            this.valuesLoaded += info.valuesLoaded;
            this.driverProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
            this.warnings.addAll(info.warnings);
        }

        public DriverCompletionInfo finish() {
            return new DriverCompletionInfo(
                documentsFound,
                valuesLoaded,
                driverProfiles,
                planProfiles,
                warnings.isEmpty() ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(warnings))
            );
        }
    }

    public static class AtomicAccumulator {
        private final AtomicLong documentsFound = new AtomicLong();
        private final AtomicLong valuesLoaded = new AtomicLong();
        private final List<DriverProfile> collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        private final List<PlanProfile> planProfiles = Collections.synchronizedList(new ArrayList<>());
        private final Set<String> warnings = Collections.synchronizedSet(new LinkedHashSet<>());

        public void accumulate(DriverCompletionInfo info) {
            this.documentsFound.addAndGet(info.documentsFound);
            this.valuesLoaded.addAndGet(info.valuesLoaded);
            this.collectedProfiles.addAll(info.driverProfiles);
            this.planProfiles.addAll(info.planProfiles);
            this.warnings.addAll(info.warnings);
        }

        public DriverCompletionInfo finish() {
            Set<String> warningsSnapshot;
            synchronized (warnings) {
                /*
                 * Preserve insertion order of the warnings so we get stuff like:
                 *   There was an error in the [BORT(a, b)], only the first 20 returned:
                 *   param a must be positive but was [-1231]
                 *   param b must be a string at least 100 characters but was [candy]
                 */
                warningsSnapshot = warnings.isEmpty() ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(warnings));
            }
            return new DriverCompletionInfo(documentsFound.get(), valuesLoaded.get(), collectedProfiles, planProfiles, warningsSnapshot);
        }
    }
}
