/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class UnmappedNonLoadableBlockLoaderTests extends ESTestCase {
    public void testMissingFieldReadsAsNull() throws IOException {
        assertThat(load("f", randomNonLoadableType(), Map.of("other", "value")), nullValue());
    }

    public void testPresentValueFails() {
        DataType dataType = randomNonLoadableType();
        Object value = randomFrom(
            List.of(81, "William Faulkner", Map.of("min", 1.0, "max", 3.0, "sum", 10.1, "value_count", 5), List.of("a", "b"))
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> load("f", dataType, Map.of("f", value)));
        assertThat(
            e.getMessage(),
            containsString(
                "Field [f] of type ["
                    + dataType.typeName()
                    + "] is unmapped in this index and has no implicit conversion from KEYWORD, so its _source value cannot be loaded"
            )
        );
    }

    private static DataType randomNonLoadableType() {
        return randomFrom(DataType.AGGREGATE_METRIC_DOUBLE, DataType.TEXT);
    }

    private static Object load(String fieldName, DataType dataType, Map<String, Object> source) throws IOException {
        UnmappedNonLoadableBlockLoader loader = new UnmappedNonLoadableBlockLoader(
            fieldName,
            dataType,
            Set.of(fieldName),
            IgnoredSourceFormat.NO_IGNORED_SOURCE
        );
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(source, XContentType.JSON)), builder);
            return ((TestBlock) builder.build()).get(0);
        }
    }

    private static BlockLoader.StoredFields storedFields(Source source) {
        return new BlockLoader.StoredFields() {
            @Override
            public Source source() {
                return source;
            }

            @Override
            public String id() {
                return "0";
            }

            @Override
            public String routing() {
                return null;
            }

            @Override
            public Map<String, List<Object>> storedFields() {
                return Map.of();
            }

            @Override
            public boolean loaded() {
                return true;
            }
        };
    }
}
