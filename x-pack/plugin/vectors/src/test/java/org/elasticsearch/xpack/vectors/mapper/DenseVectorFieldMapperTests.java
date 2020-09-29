/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.vectors.Vectors;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DenseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new Vectors());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector").field("dims", 4);
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.startArray().value(1).value(2).value(3).value(4).endArray();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("dims",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4)),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 5)));
    }

    public void testDims() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 0);
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: " +
                "The number of dimensions for field [field] should be in the range [1, 2048] but was [0]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 3000);
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: " +
                "The number of dimensions for field [field] should be in the range [1, 2048] but was [3000]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Missing required parameter [dims] for field [field]"));
        }
    }
}
