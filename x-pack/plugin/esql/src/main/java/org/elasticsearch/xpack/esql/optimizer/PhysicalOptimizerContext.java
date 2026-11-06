/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.optimizer.cbo.PlanDecisionsHolder;
import org.elasticsearch.xpack.esql.session.Configuration;

public record PhysicalOptimizerContext(Configuration configuration, TransportVersion minimumVersion, PlanDecisionsHolder decisions) {
    public PhysicalOptimizerContext(Configuration configuration, TransportVersion minimumVersion) {
        this(configuration, minimumVersion, new PlanDecisionsHolder());
    }
}
