/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.GeoPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeIndexer;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;
import org.elasticsearch.xpack.spatial.ingest.CircleProcessor;
import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class SpatialPlugin extends Plugin implements ActionPlugin, GeoPlugin, MapperPlugin, SearchPlugin, IngestPlugin {

    public SpatialPlugin(Settings settings) {
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.SPATIAL, SpatialUsageTransportAction.class),
            new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.SPATIAL, SpatialInfoTransportAction.class));
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(ShapeFieldMapper.CONTENT_TYPE, new ShapeFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(ShapeQueryBuilder.NAME, ShapeQueryBuilder::new, ShapeQueryBuilder::fromXContent));
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(CircleProcessor.TYPE, new CircleProcessor.Factory());
    }

    @Override
    public List<GeoShapeFieldMapper.CRSHandler> getCRSHandlers() {
        return List.of(new Proj4JHandler());
    }

    /**
     * Handles indexing & query building w/ proper field based on CoordinateReferenceSystem
     */
    public static class Proj4JHandler implements GeoShapeFieldMapper.CRSHandler {
        private CRSFactory projCRSFactory;
        private CoordinateReferenceSystem crs;

        public Proj4JHandler() {
            projCRSFactory = AccessController.doPrivileged((PrivilegedAction<CRSFactory>) () -> new CRSFactory());
        }

        @Override
        public AbstractGeometryFieldMapper.Indexer newIndexer(boolean orientation, String fieldName) {
            if (crs.getProjection().isGeographic() == false) {
                return new ShapeIndexer(fieldName);
            }
            return new GeoShapeIndexer(orientation, fieldName);
        }

        @Override
        public AbstractGeometryFieldMapper.QueryProcessor newQueryProcessor() {
            if (crs.getProjection().isGeographic() == false) {
                return new ShapeQueryProcessor();
            }
            return new VectorGeoShapeQueryProcessor();
        }

        @Override
        public boolean supportsCRS(String crsSpec) {
            this.crs = null;
            // test if name is a PROJ4 spec
            if (crsSpec.indexOf("+") >= 0 || crsSpec.indexOf("=") >= 0) {
                this.crs = projCRSFactory.createFromParameters("Anon", crsSpec);
            } else {
                this.crs = projCRSFactory.createFromName(crsSpec);
            }

            return this.crs != null ? true : false;
        }
    }
}
