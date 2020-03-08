/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.spi;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SpatialExtension implements GeoShapeFieldMapper.Extension, AbstractGeometryFieldMapper.ParserExtension {

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

    @Override
    public List<AbstractGeometryFieldMapper.ParserHandler> getParserExtensions() {
        return List.of(new SpatialParserHandler());
    }

    public static class SpatialParserHandler implements AbstractGeometryFieldMapper.ParserHandler {
        @Override
        public boolean parse(Iterator<Map.Entry<String, Object>> iterator, String name, Object node, Map<String, Object> params,
                             Version indexVersionCreated) {
            if (indexVersionCreated.onOrAfter(Version.V_8_0_0) && name.equals(TypeParsers.DOC_VALUES)) {
                params.put(TypeParsers.DOC_VALUES,
                    Boolean.valueOf(XContentMapValues.nodeBooleanValue(node, name + "." + TypeParsers.DOC_VALUES)));
                iterator.remove();
                return true;
            }
            return false;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void config(Map<String, java.lang.Object> params, AbstractGeometryFieldMapper.Builder builder) {
            if (params.containsKey(TypeParsers.DOC_VALUES)) {
                builder.docValues((Boolean)params.get(TypeParsers.DOC_VALUES));
            }
        }
    }

    protected static class SpatialDataHandler extends GeoShapeFieldMapper.DataHandler {
        public SpatialDataHandler() {
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
