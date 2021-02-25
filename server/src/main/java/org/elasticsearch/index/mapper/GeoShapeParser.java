/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geometry.Geometry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.Collections;

public class GeoShapeParser extends AbstractGeometryFieldMapper.Parser<Geometry> {
    private final GeometryParser geometryParser;

    public GeoShapeParser(GeometryParser geometryParser) {
        this.geometryParser = geometryParser;
    }

    @Override
    public Geometry parse(XContentParser parser) throws IOException, ParseException {
        return geometryParser.parse(parser);
    }

    @Override
    public Object format(Geometry value, String format) {
        return geometryParser.geometryFormat(format).toXContentAsObject(value);
    }

    @Override
    public Object parseAndFormatObject(Object value, String format) {
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            Collections.singletonMap("dummy_field", value), XContentType.JSON)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value

            GeometryFormat<Geometry> geometryFormat = geometryParser.geometryFormat(parser);
            if (geometryFormat.name().equals(format)) {
                return value;
            }

            Geometry geometry = geometryFormat.fromXContent(parser);
            return format(geometry, format);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
