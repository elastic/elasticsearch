/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.spi;

import org.elasticsearch.Version;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collections;
import java.util.Map;

public class SpatialExtension implements GeoShapeFieldMapper.Extension {

    public SpatialExtension() {
    }

    public static XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public Map<String, GeoShapeFieldMapper.DataHandlerFactory> getDataHandlerFactories() {
        XPackLicenseState licenseState = getLicenseState();
        if (licenseState != null && licenseState.isSpatialAllowed()) {
            // HACK: override the default data handler factory, this is trappy because other plugins could override
            GeoShapeFieldMapper.DATA_HANDLER_FACTORIES.replace(
                GeoShapeFieldMapper.Defaults.DATA_HANDLER.value(), () -> new SpatialDataHandler());
        }
        return Collections.emptyMap();
    }

    protected static class SpatialDataHandler extends GeoShapeFieldMapper.DataHandler {
        public SpatialDataHandler() {
        }

        @Override
        public boolean supportsDocValues() {
            return true;
        }

        @Override
        public boolean defaultDocValues(Version indexVersionCreated) {
            // indexVersionCreated used for bwc
            return true;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractGeometryFieldMapper.Indexer newIndexer(boolean orientation, MappedFieldType fieldType) {
            return new GeoShapeIndexer(orientation, fieldType.name()) {
                @Override
                public void indexDocValues(ParseContext context, Geometry shape) {
                    // indexing doc values
                    /** @todo add geo_shape doc values */
                    //  context.doc().add(new GeoShapeDocValueField(....))
                }
            };
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractGeometryFieldMapper.QueryProcessor newQueryProcessor() {
            /** @todo overload VectorGeoShapeQueryProcessor to support GeoShape doc value queries; licensed gold */
            return new VectorGeoShapeQueryProcessor();
        }
    }
}
