/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.existence;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.autoscaling.shards.LocalStateAutoscalingAndSearchableSnapshots;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

/**
 * We need a local state plugin including both searchable snapshots and ilm in order to verify the frozen 0-1 case works through ilm.
 * The local state plugin is necessary to avoid touching the "static SetOnce" licenseState field in XPackPlugin.
 */
public class LocalStateAutoscalingAndSearchableSnapshotsAndIndexLifecycle extends LocalStateAutoscalingAndSearchableSnapshots {

    public LocalStateAutoscalingAndSearchableSnapshotsAndIndexLifecycle(final Settings settings) {
        super(settings);
        plugins.add(new IndexLifecycle(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateAutoscalingAndSearchableSnapshotsAndIndexLifecycle.this.getLicenseState();
            }
        });
    }
}
