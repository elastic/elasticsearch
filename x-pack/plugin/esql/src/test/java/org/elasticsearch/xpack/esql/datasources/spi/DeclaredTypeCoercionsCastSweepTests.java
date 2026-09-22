/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link DeclaredTypeCoercions#supports} is the castability predicate resolution consults, and
 * {@link DeclaredTypeCoercions#castBlock} is what the columnar readers run for every supported pair that is not fused
 * into the decode. A pair the predicate admits and the cast cannot perform would pass resolution and then fail or null
 * at read, so this sweeps every admitted pair over the decodable types rather than naming them one at a time.
 */
public class DeclaredTypeCoercionsCastSweepTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private static final List<DataType> DECODABLE = List.of(
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.IP,
        DataType.UNSIGNED_LONG
    );

    /** A string token each target parses without loss. */
    private static final Map<DataType, String> TOKEN_FOR_TARGET = Map.of(
        DataType.KEYWORD,
        "x",
        DataType.TEXT,
        "x",
        DataType.INTEGER,
        "1",
        DataType.LONG,
        "1",
        DataType.DOUBLE,
        "1",
        DataType.BOOLEAN,
        "true",
        DataType.DATETIME,
        "2020-01-01T00:00:00Z",
        DataType.DATE_NANOS,
        "2020-01-01T00:00:00Z",
        DataType.IP,
        "10.0.0.1",
        DataType.UNSIGNED_LONG,
        "1"
    );

    public void testEverySupportedPairCasts() {
        List<String> failures = new ArrayList<>();
        for (DataType from : DECODABLE) {
            for (DataType to : DECODABLE) {
                if (from == to || DeclaredTypeCoercions.supports(from, to) == false) {
                    continue;
                }
                List<String> warnings = new ArrayList<>();
                try (Block source = oneValue(from, to)) {
                    try (Block cast = DeclaredTypeCoercions.castBlock(source, from, to, null, blockFactory, "col", new SkipWarnings("s") {
                        @Override
                        public void add(String detail) {
                            warnings.add(detail);
                        }
                    })) {
                        if (cast.isNull(0)) {
                            failures.add(from.typeName() + "->" + to.typeName() + " produced null " + warnings);
                        } else if (cast.elementType() != DeclaredTypeCoercions.elementTypeFor(to)) {
                            failures.add(from.typeName() + "->" + to.typeName() + " produced " + cast.elementType());
                        }
                    }
                } catch (RuntimeException e) {
                    failures.add(from.typeName() + "->" + to.typeName() + " threw " + e);
                }
            }
        }
        assertTrue("supported pairs castBlock cannot perform: " + failures, failures.isEmpty());
    }

    private Block oneValue(DataType from, DataType to) {
        return switch (from) {
            case KEYWORD, TEXT -> bytes(TOKEN_FOR_TARGET.get(to));
            case IP -> {
                try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(1)) {
                    b.appendBytesRef(StringUtils.parseIP("10.0.0.1"));
                    yield b.build();
                }
            }
            case INTEGER -> blockFactory.newIntArrayVector(new int[] { 1 }, 1).asBlock();
            case LONG, DATETIME, DATE_NANOS -> blockFactory.newLongArrayVector(new long[] { 1L }, 1).asBlock();
            case UNSIGNED_LONG -> blockFactory.newLongArrayVector(new long[] { NumericUtils.asLongUnsigned(1L) }, 1).asBlock();
            case DOUBLE -> blockFactory.newDoubleArrayVector(new double[] { 1.0 }, 1).asBlock();
            case BOOLEAN -> blockFactory.newBooleanArrayVector(new boolean[] { true }, 1).asBlock();
            default -> throw new AssertionError("no fixture for " + from);
        };
    }

    private Block bytes(String value) {
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(new BytesRef(value));
            return b.build();
        }
    }
}
