/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;

class ShapeParser extends AbstractGeometryFieldMapper.Parser<Geometry> {
    private final GeometryParser geometryParser;

    ShapeParser(GeometryParser geometryParser) {
        this.geometryParser = geometryParser;
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
        return geometry;
    }
}
