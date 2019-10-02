/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.spi;

import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.projections.Proj4JHandlerFactory;

import java.util.Collections;
import java.util.List;

public class SpatialExtension implements GeoShapeFieldMapper.Extension {
    public SpatialExtension() {
    }

    public static XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public List<GeoShapeFieldMapper.CRSHandlerFactory> getCRSHandlerFactories() {
        XPackLicenseState licenseState = getLicenseState();
        if (licenseState != null && licenseState.isSpatialProjectionAllowed()) {
            return List.of(new Proj4JHandlerFactory());
        }
        return Collections.emptyList();
    }
}
