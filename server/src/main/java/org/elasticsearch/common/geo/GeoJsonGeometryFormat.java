/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;

import java.io.IOException;
import java.io.UncheckedIOException;

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
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            GeoJson.toXContent(geometry, builder, ToXContent.EMPTY_PARAMS);
            StreamInput input = BytesReference.bytes(builder).streamInput();

            try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, input)) {
                return parser.map();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
