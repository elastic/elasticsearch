/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.geometry.Geometry;


public interface VectorTileExtension {
    /**
     * Get the vector tile engine. This is called when user ask for the MVT format on the field API.
     * We are only expecting one instance of a vector tile engine coming from the vector tile module.
     */
    GeoFormatterFactory.VectorTileEngine<Geometry> getVectorTileEngine();
}
