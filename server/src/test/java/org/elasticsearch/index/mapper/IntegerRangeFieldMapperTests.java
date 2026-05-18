/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;

public class IntegerRangeFieldMapperTests extends RangeFieldMapperTests {
    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return rangeSource(in, "1", "3");
    }

    @Override
    protected String storedValue() {
        return "1 : 3";
    }

    @Override
    protected Object rangeValue() {
        return 2;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "integer_range");
    }

    @Override
    protected TestRange<Integer> randomRangeForSyntheticSourceTest() {
        var includeFrom = randomBoolean();
        Integer from = randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - 1);
        var includeTo = randomBoolean();
        Integer to = randomIntBetween((from) + 1, Integer.MAX_VALUE);

        if (rarely()) {
            from = null;
        }
        if (rarely()) {
            to = null;
        }

        return new TestRange<>(rangeType(), from, to, includeFrom, includeTo);
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.INTEGER;
    }

    /**
     * Test that block loader returns constant nulls when doc_values is disabled for a non-DATE range type.
     * This is the behavior after reordering the checks in {@code blockLoader}: doc_values is checked before
     * range type, so a non-DATE range with doc_values disabled returns null rather than throwing.
     */
    public void testNoDocValuesBlockLoader() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "integer_range");
            b.field("doc_values", false);
            b.endObject();
        }));
        BlockLoader loader = mapper.fieldType("field").blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapper));
        assertSame(ConstantNull.INSTANCE, loader);
    }

    /**
     * Test that a non-DATE range type with doc_values enabled still throws UnsupportedOperationException,
     * since only date_range supports block loading via doc values.
     */
    public void testBlockLoaderWithDocValues() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "integer_range");
            b.endObject();
        }));
        expectThrows(
            UnsupportedOperationException.class,
            () -> mapper.fieldType("field").blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapper))
        );
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
