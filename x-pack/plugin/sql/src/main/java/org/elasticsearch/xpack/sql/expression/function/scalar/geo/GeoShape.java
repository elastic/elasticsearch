/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Wrapper class to represent a GeoShape in SQL
 *
 * It is required to override the XContent serialization. The ShapeBuilder serializes using GeoJSON by default,
 * but in SQL we need the serialization to be WKT-based.
 */
public class GeoShape implements ToXContentFragment {

    private final ShapeBuilder<?, ?> shapeBuilder;

    public GeoShape(Object value) throws IOException {
        shapeBuilder = ShapeParser.parse(value);
    }

    @Override
    public String toString() {
        return shapeBuilder.toWKT();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(shapeBuilder.toWKT());
    }
}
