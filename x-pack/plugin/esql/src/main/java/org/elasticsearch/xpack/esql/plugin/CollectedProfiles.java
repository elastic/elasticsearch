/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.xpack.esql.planner.PlannerProfile;

import java.io.IOException;
import java.util.List;

/**
 * Holds the collection of driver profiles and query planning profiles from the individual nodes processing the query.
 */
public class CollectedProfiles implements Writeable {
    public static final CollectedProfiles EMPTY = new CollectedProfiles(List.of(), List.of());

    private List<DriverProfile> driverProfiles;
    private List<PlannerProfile> plannerProfiles;

    public CollectedProfiles(List<DriverProfile> driverProfiles, List<PlannerProfile> plannerProfiles) {
        this.driverProfiles = driverProfiles;
        this.plannerProfiles = plannerProfiles;
    }

    public CollectedProfiles(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public List<DriverProfile> getDriverProfiles() {
        return driverProfiles;
    }

    public List<PlannerProfile> getPlannerProfiles() {
        return plannerProfiles;
    }
}
