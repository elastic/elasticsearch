/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.geometry.Geometry;

import java.util.List;

/**
 * Utility that converts geometries into Lucene-compatible form for indexing in a shape or geo_shape field.
 * Implementing classes handle the specifics for converting either geo_shape into LatLon lucene index format
 * or shape into XY lucene format.
 */
public interface ShapeIndexer {
    List<IndexableField> indexShape(Geometry geometry);
}
