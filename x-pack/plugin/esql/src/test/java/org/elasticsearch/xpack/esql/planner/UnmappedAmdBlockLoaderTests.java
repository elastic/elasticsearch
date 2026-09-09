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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UnmappedAmdBlockLoaderTests extends ESTestCase {
    public void testFullAmdObject() throws IOException {
        assertThat(load(Map.of("f", Map.of("min", 1, "max", 4, "sum", 10, "value_count", 3))), equalTo(amd(1.0, 4.0, 10.0, 3)));
    }

    public void testPartialMetricsNullFillMissingKeys() throws IOException {
        assertThat(load(Map.of("f", Map.of("min", 1, "max", 2))), equalTo(amd(1.0, 2.0, null, null)));
    }

    public void testNonObjectThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> load(Map.of("f", 81)));
        assertThat(e.getMessage(), containsString("Cannot load field [f] as aggregate_metric_double from _source; got [81]"));
    }

    public void testDoesNotSupportOrdinals() {
        UnmappedAmdBlockLoader loader = loader("f");
        assertThat(loader.supportsOrdinals(), equalTo(false));
        expectThrows(UnsupportedOperationException.class, () -> loader.ordinals(null));
    }

    public void testNoColumnAtATimeReader() {
        assertThat(loader("f").columnAtATimeReader(null), nullValue());
    }

    public void testToStringNamesTheFieldAndIsDistinctFromTheReader() throws IOException {
        UnmappedAmdBlockLoader loader = loader("some_field");
        assertThat(loader.toString(), equalTo("UnmappedAmdBlockLoader[some_field]"));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            assertThat(reader.toString(), equalTo("UnmappedAmdBlockLoader.Reader[some_field]"));
        }
    }

    private static UnmappedAmdBlockLoader loader(String fieldName) {
        return new UnmappedAmdBlockLoader(fieldName, Set.of(fieldName), IgnoredSourceFormat.NO_IGNORED_SOURCE);
    }

    private static Object load(Map<String, Object> sourceMap) throws IOException {
        UnmappedAmdBlockLoader loader = loader("f");
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(sourceMap, XContentType.JSON)), builder);
            return ((TestBlock) builder.build()).get(0);
        }
    }

    private static Map<String, Object> amd(Double min, Double max, Double sum, Integer count) {
        Map<String, Object> value = new HashMap<>();
        value.put("min", min);
        value.put("max", max);
        value.put("sum", sum);
        value.put("value_count", count);
        return value;
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
