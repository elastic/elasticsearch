/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.geometry.Geometry;

import java.util.List;

/**
 * Extension point for geometry formatters
 */
public interface GeometryFormatterExtension {
    /**
     * Get a list of geometry formatters.
     */
    List<GeoFormatterFactory.FormatterFactory<Geometry>> getGeometryFormatterFactories();
}
