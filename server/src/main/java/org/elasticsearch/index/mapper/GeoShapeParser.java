/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;

public class GeoShapeParser extends AbstractGeometryFieldMapper.Parser<Geometry> {
    private final GeometryParser geometryParser;
    private final Orientation orientation;

    public GeoShapeParser(GeometryParser geometryParser, Orientation orientation) {
        this.geometryParser = geometryParser;
        this.orientation = orientation;
    }

    @Override
    public void parse(
        XContentParser parser,
        CheckedConsumer<Geometry, IOException> consumer,
        AbstractGeometryFieldMapper.MalformedValueHandler malformedHandler
    ) throws IOException {
        try {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    parse(parser, consumer, malformedHandler);
                }
            } else {
                consumer.accept(geometryParser.parse(parser));
            }
        } catch (ParseException | ElasticsearchParseException | IllegalArgumentException e) {
            malformedHandler.notify(e);
        }
    }

    @Override
    public Geometry normalizeFromSource(Geometry geometry) {
        // GeometryNormalizer contains logic for validating the input geometry,
        // so it needs to be run always at indexing time. When run over source we can skip
        // the validation, and we run normalization (which is expensive) only when we need
        // to split geometries around the dateline.
        if (GeometryNormalizer.needsNormalize(orientation, geometry)) {
            return GeometryNormalizer.apply(orientation, geometry);
        } else {
            return geometry;
        }
    }
}
