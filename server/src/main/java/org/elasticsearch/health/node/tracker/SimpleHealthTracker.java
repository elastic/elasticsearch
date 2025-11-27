/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.health.node.SimpleHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

/**
 * A base class for health trackers that report {@link SimpleHealthInfo}.
 * These trackers run on every node and report their status to the health node
 * where they are cached to be later retrieved as part of the `_health_report` API.
 */
public abstract class SimpleHealthTracker extends HealthTracker<SimpleHealthInfo> {

    public abstract String trackerName();

    public abstract String greenSymptom();

    public abstract String yellowSymptom();

    public abstract String redSymptom();

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, SimpleHealthInfo healthInfo) {
        builder.simpleHealthInfoByTrackerName(trackerName(), healthInfo);
    }
}
