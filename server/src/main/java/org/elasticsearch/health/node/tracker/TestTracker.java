/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.SimpleHealthInfo;

import java.util.Collections;

public class TestTracker extends SimpleHealthTracker {
    @Override
    public String trackerName() {
        return "test-tracker-abs";
    }

    @Override
    public String greenSymptom() {
        return "All is well";
    }

    @Override
    public String yellowSymptom() {
        return "Bit bugged";
    }

    @Override
    public String redSymptom() {
        return "very bugged";
    }

    @Override
    protected SimpleHealthInfo determineCurrentHealth() {
        return new SimpleHealthInfo(HealthStatus.YELLOW, "test details from abs", Collections.emptyMap());
    }
}
