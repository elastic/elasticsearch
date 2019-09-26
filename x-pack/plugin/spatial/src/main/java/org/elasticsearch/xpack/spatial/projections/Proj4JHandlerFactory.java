/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.projections;

import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper.CRSHandler;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper.CRSHandlerFactory;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeIndexer;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;
import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransformFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class Proj4JHandlerFactory implements CRSHandlerFactory {
    private CRSFactory projCRSFactory;

    public Proj4JHandlerFactory() {
        this.projCRSFactory = AccessController.doPrivileged((PrivilegedAction<CRSFactory>) () -> new CRSFactory());
    }

    @Override
    public Proj4JHandler newCRSHandler(String crsSpec) {
        CoordinateReferenceSystem crs;
        try {
            // test if name is a PROJ4 spec
            if (crsSpec.indexOf("+") >= 0 || crsSpec.indexOf("=") >= 0) {
                crs = projCRSFactory.createFromParameters("Anon", crsSpec);
            } else {
                crs = projCRSFactory.createFromName(crsSpec);
            }
        } catch (Exception e) {
            // yes; this is gross, but we currently don't have a registry
            // to verify if the CRS is actually supported by the factory
            // it's a limitation of Proj4J and will need some contribution upstream
            return null;
        }
        return crs == null ? null : new Proj4JHandler(crs, projCRSFactory);
    }


    public static class Proj4JHandler implements CRSHandler {
        private CRSFactory projCRSFactory;
        private CoordinateTransformFactory projCTFactory;
        private CoordinateReferenceSystem crs;

        protected Proj4JHandler(CoordinateReferenceSystem crs, CRSFactory projCRSFactory) {
            this.projCRSFactory = projCRSFactory;
            this.projCTFactory = AccessController.doPrivileged(((PrivilegedAction<CoordinateTransformFactory>) () -> new CoordinateTransformFactory()));
            this.crs = crs;
        }

        @Override
        public AbstractGeometryFieldMapper.Indexer newIndexer(boolean orientation, String fieldName) {
            if (crs.isGeographic() == false) {
                return new ShapeIndexer(fieldName);
            }
            return new GeoShapeIndexer(orientation, fieldName);
        }

        @Override
        public AbstractGeometryFieldMapper.QueryProcessor newQueryProcessor() {
            if (crs.isGeographic() == false) {
                return new ShapeQueryProcessor();
            }
            return new VectorGeoShapeQueryProcessor();
        }

        @Override
        public CoordinateReferenceSystem resolveCRS(String crsSpec) {
            // test if name is a PROJ4 spec
            if (crsSpec.indexOf("+") >= 0 || crsSpec.indexOf("=") >= 0) {
                return projCRSFactory.createFromParameters("Anon", crsSpec);
            }
            return projCRSFactory.createFromName(crsSpec);
        }
    }
}
