/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Geometry serializer/deserializer
 */
public interface GeometryFormat<ParsedFormat> {

    /**
     * The name of the format, for example 'wkt'.
     */
    String name();

    /**
     * Parser JSON representation of a geometry
     */
    ParsedFormat fromXContent(XContentParser parser) throws IOException, ParseException;

    /**
     * Serializes the geometry into its JSON representation
     */
    XContentBuilder toXContent(ParsedFormat geometry, XContentBuilder builder, ToXContent.Params params) throws IOException;

    /**
     * Serializes the geometry into a standard Java object.
     *
     * For example, the GeoJson format returns the geometry as a map, while WKT returns a string.
     */
    Object toXContentAsObject(ParsedFormat geometry);
}
