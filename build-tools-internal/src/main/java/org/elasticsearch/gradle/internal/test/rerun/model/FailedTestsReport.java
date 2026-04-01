/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * This reflects the model provided by develocity api call
 * `api/tests/build/<buildId>?testOutcomes=failed`
 * */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailedTestsReport(List<WorkUnit> workUnits) {

    public FailedTestsReport(List<WorkUnit> workUnits) {
        this.workUnits = workUnits != null ? workUnits : java.util.Collections.emptyList();
    }
}
