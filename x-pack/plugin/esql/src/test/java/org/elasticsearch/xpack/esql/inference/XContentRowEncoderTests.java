/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0;
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class XContentRowEncoderTests extends ComputeTestCase {

    public void testEncode() {
        // Create a DriverContext, which is required by the encoder
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory);

        // Define the fields to be encoded, along with mock evaluators that will produce the data
        Map<ColumnInfoImpl, EvalOperator.ExpressionEvaluator.Factory> fields = new LinkedHashMap<>();
        fields.put(new ColumnInfoImpl("field1", DataType.KEYWORD, Collections.emptyList()), c -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                var builder = blockFactory.newBytesRefBlockBuilder(page.getPositionCount());
                builder.appendBytesRef(new BytesRef("value1"));
                builder.appendBytesRef(new BytesRef("value2"));
                builder.appendNull();
                builder.appendNull();
                return builder.build();
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        });
        fields.put(new ColumnInfoImpl("field2", DataType.INTEGER, Collections.emptyList()), c -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                var builder = blockFactory.newIntBlockBuilder(page.getPositionCount());
                builder.appendInt(42);
                builder.appendNull();
                builder.appendInt(43);
                builder.appendNull();
                return builder.build();
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        });
        fields.put(new ColumnInfoImpl("field3", DataType.BOOLEAN, Collections.emptyList()), c -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                var builder = blockFactory.newBooleanBlockBuilder(page.getPositionCount());
                builder.appendNull();
                builder.appendBoolean(true);
                builder.appendBoolean(false);
                builder.appendNull();
                return builder.build();
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        });

        // Create the encoder instance
        XContentRowEncoder.Factory factory = new XContentRowEncoder.Factory(XContentType.JSON, fields);
        XContentRowEncoder encoder = factory.get(driverContext);

        // Create a page of data to be encoded
        Block[] blocks = new Block[0];
        try (Page page = new Page(4, blocks); BytesRefBlock result = encoder.eval(page);) {
            BytesRef scratch = new BytesRef();
            assertThat(result.getPositionCount(), equalTo(4));

            // Checking content for position that contains some non null values.
            assertThat(result.getBytesRef(0, scratch).utf8ToString(), equalTo("{\"field1\":\"value1\",\"field2\":42}"));
            assertThat(result.getBytesRef(1, scratch).utf8ToString(), equalTo("{\"field1\":\"value2\",\"field3\":true}"));
            assertThat(result.getBytesRef(2, scratch).utf8ToString(), equalTo("{\"field2\":43,\"field3\":false}"));

            // Position 4 contains only null values: returning null
            assertThat(result.isNull(3), equalTo(true));
        }
    }
}
