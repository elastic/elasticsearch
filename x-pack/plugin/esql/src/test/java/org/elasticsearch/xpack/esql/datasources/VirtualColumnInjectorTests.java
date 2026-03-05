/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VirtualColumnInjectorTests extends ESTestCase {

    private final BlockFactory blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);

    public void testTwoDataTwoPartitionColumns() {
        List<Attribute> fullOutput = List.of(
            attr("emp_no", DataType.INTEGER),
            attr("name", DataType.KEYWORD),
            partAttr("year", DataType.INTEGER),
            partAttr("region", DataType.KEYWORD)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year", "region"));
        Map<String, Object> partitionValues = Map.of("year", 2024, "region", "us-east");

        VirtualColumnInjector injector = new VirtualColumnInjector(fullOutput, partitionCols, partitionValues, blockFactory);

        assertTrue(injector.hasPartitionColumns());
        assertEquals(List.of("emp_no", "name"), injector.dataColumnNames());

        // Create a data page with 2 data columns, 3 rows
        IntBlock empNoBlock = blockFactory.newConstantIntBlockWith(42, 3);
        BytesRefBlock nameBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("Alice"), 3);
        Page dataPage = new Page(3, new Block[] { empNoBlock, nameBlock });

        Page result = injector.inject(dataPage);

        assertEquals(3, result.getPositionCount());
        assertEquals(4, result.getBlockCount());

        // emp_no at position 0
        IntBlock resultEmpNo = result.getBlock(0);
        assertEquals(42, resultEmpNo.getInt(0));

        // name at position 1
        BytesRefBlock resultName = result.getBlock(1);
        assertEquals(new BytesRef("Alice"), resultName.getBytesRef(0, new BytesRef()));

        // year at position 2
        IntBlock resultYear = result.getBlock(2);
        assertEquals(2024, resultYear.getInt(0));
        assertEquals(2024, resultYear.getInt(2));

        // region at position 3
        BytesRefBlock resultRegion = result.getBlock(3);
        assertEquals(new BytesRef("us-east"), resultRegion.getBytesRef(0, new BytesRef()));
    }

    public void testAllBlockTypes() {
        List<Attribute> fullOutput = List.of(
            attr("data", DataType.INTEGER),
            partAttr("intCol", DataType.INTEGER),
            partAttr("longCol", DataType.LONG),
            partAttr("doubleCol", DataType.DOUBLE),
            partAttr("boolCol", DataType.BOOLEAN),
            partAttr("keywordCol", DataType.KEYWORD)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of("intCol", "longCol", "doubleCol", "boolCol", "keywordCol"));
        Map<String, Object> partitionValues = Map.of(
            "intCol",
            42,
            "longCol",
            9999999999L,
            "doubleCol",
            3.14,
            "boolCol",
            true,
            "keywordCol",
            "hello"
        );

        VirtualColumnInjector injector = new VirtualColumnInjector(fullOutput, partitionCols, partitionValues, blockFactory);

        IntBlock dataBlock = blockFactory.newConstantIntBlockWith(1, 2);
        Page dataPage = new Page(2, new Block[] { dataBlock });

        Page result = injector.inject(dataPage);

        assertEquals(2, result.getPositionCount());
        assertEquals(6, result.getBlockCount());

        IntBlock intResult = result.getBlock(1);
        assertEquals(42, intResult.getInt(0));

        LongBlock longResult = result.getBlock(2);
        assertEquals(9999999999L, longResult.getLong(0));

        DoubleBlock doubleResult = result.getBlock(3);
        assertEquals(3.14, doubleResult.getDouble(0), 0.001);

        BooleanBlock boolResult = result.getBlock(4);
        assertTrue(boolResult.getBoolean(0));

        BytesRefBlock keywordResult = result.getBlock(5);
        assertEquals(new BytesRef("hello"), keywordResult.getBytesRef(0, new BytesRef()));
    }

    public void testEmptyPageZeroPositions() {
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr("year", DataType.INTEGER));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
        Map<String, Object> partitionValues = Map.of("year", 2024);

        VirtualColumnInjector injector = new VirtualColumnInjector(fullOutput, partitionCols, partitionValues, blockFactory);

        IntBlock emptyBlock = blockFactory.newConstantIntBlockWith(0, 0);
        Page dataPage = new Page(0, new Block[] { emptyBlock });

        Page result = injector.inject(dataPage);

        assertEquals(0, result.getPositionCount());
        assertEquals(2, result.getBlockCount());
    }

    public void testNoPartitionColumnsReturnsPageUnchanged() {
        List<Attribute> fullOutput = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        Set<String> partitionCols = Set.of();

        VirtualColumnInjector injector = new VirtualColumnInjector(fullOutput, partitionCols, Map.of(), blockFactory);

        assertFalse(injector.hasPartitionColumns());

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(1, 2);
        BytesRefBlock bBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("x"), 2);
        Page dataPage = new Page(2, new Block[] { aBlock, bBlock });

        Page result = injector.inject(dataPage);
        assertSame(dataPage, result);
    }

    public void testColumnOrderingDataThenPartition() {
        List<Attribute> fullOutput = List.of(
            attr("a", DataType.INTEGER),
            attr("b", DataType.KEYWORD),
            partAttr("year", DataType.INTEGER),
            partAttr("month", DataType.INTEGER)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year", "month"));
        Map<String, Object> partitionValues = Map.of("year", 2024, "month", 6);

        VirtualColumnInjector injector = new VirtualColumnInjector(fullOutput, partitionCols, partitionValues, blockFactory);

        assertEquals(List.of("a", "b"), injector.dataColumnNames());

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(10, 1);
        BytesRefBlock bBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("test"), 1);
        Page dataPage = new Page(1, new Block[] { aBlock, bBlock });

        Page result = injector.inject(dataPage);

        assertEquals(4, result.getBlockCount());
        IntBlock yearBlock = result.getBlock(2);
        assertEquals(2024, yearBlock.getInt(0));
        IntBlock monthBlock = result.getBlock(3);
        assertEquals(6, monthBlock.getInt(0));
    }

    public void testConstantValuesAcrossAllPositions() {
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr("year", DataType.INTEGER));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
        Map<String, Object> partitionValues = Map.of("year", 2024);

        VirtualColumnInjector injector = new VirtualColumnInjector(fullOutput, partitionCols, partitionValues, blockFactory);

        IntBlock dataBlock = blockFactory.newConstantIntBlockWith(1, 5);
        Page dataPage = new Page(5, new Block[] { dataBlock });

        Page result = injector.inject(dataPage);

        IntBlock yearBlock = result.getBlock(1);
        for (int i = 0; i < 5; i++) {
            assertEquals(2024, yearBlock.getInt(i));
        }
    }

    private static Attribute attr(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Attribute partAttr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }
}
