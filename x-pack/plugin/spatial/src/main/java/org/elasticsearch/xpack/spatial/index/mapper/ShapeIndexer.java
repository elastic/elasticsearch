/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;

public class ShapeIndexer implements AbstractGeometryFieldMapper.Indexer<Geometry, Geometry> {

    @Override
    public Geometry prepareForIndexing(Geometry geometry) {
        return geometry;
    }

    @Override
    public Class<Geometry> processedClass() {
        return Geometry.class;
    }
}
