/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.InternalVectorFormatProviderPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;

public class DiskBBQPlugin extends Plugin implements InternalVectorFormatProviderPlugin {

    public static final LicensedFeature.Momentary DISK_BBQ_FEATURE = LicensedFeature.momentary(
        "vector-search",
        "bbq_disk",
        License.OperationMode.ENTERPRISE
    );

    public DiskBBQPlugin(Settings settings) {}

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public VectorsFormatProvider getVectorsFormatProvider() {
        return (indexSettings, options, similarity, elementType) -> {
            if (options instanceof DenseVectorFieldMapper.BBQIVFIndexOptions diskbbq) {
                if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.DISK_BBQ_LICENSE_ENFORCEMENT)
                    && DISK_BBQ_FEATURE.check(getLicenseState()) == false) {
                    throw LicenseUtils.newComplianceException(DISK_BBQ_FEATURE.getName());
                }
                int clusterSize = diskbbq.getClusterSize();
                boolean onDiskRescore = diskbbq.isOnDiskRescore();
                if (Build.current().isSnapshot()) {
                    return new ESNextDiskBBQVectorsFormat(
                        ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
                        clusterSize,
                        ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                        elementType,
                        onDiskRescore
                    );
                }
                return new ES920DiskBBQVectorsFormat(
                    clusterSize,
                    ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                    elementType,
                    onDiskRescore
                );
            }
            return null;
        };
    }

}
