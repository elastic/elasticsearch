/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Map;

public class LocalStateRankVectors extends LocalStateCompositeXPackPlugin {

    private final RankVectorsPlugin rankVectorsPlugin;
    private final XPackLicenseState licenseState = new XPackLicenseState(
        System::currentTimeMillis,
        new XPackLicenseStatus(License.OperationMode.TRIAL, true, null)
    );

    public LocalStateRankVectors(Settings settings) {
        super(settings, null);
        LocalStateRankVectors thisVar = this;
        rankVectorsPlugin = new RankVectorsPlugin() {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return rankVectorsPlugin.getMappers();
    }
}
