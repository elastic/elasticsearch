/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;

import java.io.IOException;
import java.text.ParseException;
import java.util.function.Consumer;

public class GeoShapeParser extends AbstractGeometryFieldMapper.Parser<Geometry> {
    private final GeometryParser geometryParser;

    public GeoShapeParser(GeometryParser geometryParser) {
        this.geometryParser = geometryParser;
    }

    @Override
    public void parse(
        XContentParser parser,
        CheckedConsumer<Geometry, IOException> consumer,
        Consumer<Exception> onMalformed
    ) throws IOException {
        try {
            consumer.accept(geometryParser.parse(parser));
        } catch (ParseException | ElasticsearchParseException | IllegalArgumentException e) {
            onMalformed.accept(e);
        }
    }

    @Override
    public Object format(Geometry value, String format) {
        return geometryParser.geometryFormat(format).toXContentAsObject(value);
    }

}
