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
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

public class WKTGeometryFormat implements GeometryFormat<Geometry> {
    public static final String NAME = "wkt";

    private final WellKnownText wellKnownTextParser;

    public WKTGeometryFormat(WellKnownText wellKnownTextParser) {
        this.wellKnownTextParser = wellKnownTextParser;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Geometry fromXContent(XContentParser parser) throws IOException, ParseException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        return wellKnownTextParser.fromWKT(parser.text());
    }

    @Override
    public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (geometry != null) {
            return builder.value(wellKnownTextParser.toWKT(geometry));
        } else {
            return builder.nullValue();
        }
    }

    @Override
    public String toXContentAsObject(Geometry geometry) {
        return wellKnownTextParser.toWKT(geometry);
    }
}
