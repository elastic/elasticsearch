/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.shards;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.autoscaling.LocalStateAutoscaling;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

/**
 * We need a local state plugin for searchable snapshots too (test sources are not exposed).
 * The local state plugin is necessary to avoid touching the "static SetOnce" licenseState field in XPackPlugin.
 */
public class LocalStateAutoscalingAndSearchableSnapshots extends LocalStateAutoscaling {

    public LocalStateAutoscalingAndSearchableSnapshots(final Settings settings) {
        super(settings);
        plugins.add(new SearchableSnapshots(settings) {

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateAutoscalingAndSearchableSnapshots.this.getLicenseState();
            }

        });
    }

}
