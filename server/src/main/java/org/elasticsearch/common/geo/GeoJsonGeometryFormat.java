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
import org.elasticsearch.geometry.Geometry;

import java.io.IOException;

public class GeoJsonGeometryFormat implements GeometryFormat<Geometry> {
    public static final String NAME = "geojson";

    private final GeoJson geoJsonParser;

    public GeoJsonGeometryFormat(GeoJson geoJsonParser) {
        this.geoJsonParser = geoJsonParser;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Geometry fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        return geoJsonParser.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (geometry != null) {
            return GeoJson.toXContent(geometry, builder, params);
        } else {
            return builder.nullValue();
        }
    }

    @Override
    public Object toXContentAsObject(Geometry geometry) {
        return GeoJson.toMap(geometry);
    }
}
