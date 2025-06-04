/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;

import java.io.IOException;

public class StWkttosqlProcessor implements Processor {

    static final StWkttosqlProcessor INSTANCE = new StWkttosqlProcessor();

    public static final String NAME = "geo_wkttosql";

    StWkttosqlProcessor() {}

    public StWkttosqlProcessor(StreamInput in) throws IOException {}

    @Override
    public Object process(Object input) {
        return StWkttosqlProcessor.apply(input);
    }

    public static GeoShape apply(Object input) {
        if (input == null) {
            return null;
        }

        if ((input instanceof String) == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", input);
        }
        try {
            return new GeoShape(input);
        } catch (IOException | IllegalArgumentException | ElasticsearchParseException ex) {
            throw new InvalidArgumentException("Cannot parse [{}] as a geo_shape value", input);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
