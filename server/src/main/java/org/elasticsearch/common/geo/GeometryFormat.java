/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;

import java.io.IOException;
import java.text.ParseException;

/**
 * Geometry parser
 */
public interface GeometryFormat {

    /**
     * The name of the format, for example 'wkt'.
     */
    String name();

    /**
     * Parser JSON representation of a geometry
     */
    Geometry fromXContent(XContentParser parser) throws IOException, ParseException;
}
