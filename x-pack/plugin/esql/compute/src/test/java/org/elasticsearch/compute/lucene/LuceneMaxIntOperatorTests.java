/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LuceneMaxIntOperatorTests extends LuceneMaxOperatorTestCase {

    @Override
    public LuceneMaxFactory.NumberType getNumberType() {
        return LuceneMaxFactory.NumberType.INTEGER;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            int max = Integer.MIN_VALUE;

            @Override
            public IndexableField newPointField() {
                return new IntField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            private int newValue() {
                final int value = randomInt();
                max = Math.max(max, value);
                return value;
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, newValue());
            }

            @Override
            public void assertPage(Page page) {
                assertThat(page.getBlock(0), instanceOf(IntBlock.class));
                final IntBlock db = page.getBlock(0);
                assertThat(page.getBlock(1), instanceOf(BooleanBlock.class));
                final BooleanBlock bb = page.getBlock(1);
                if (bb.getBoolean(0) == false) {
                    assertThat(db.getInt(0), equalTo(Integer.MIN_VALUE));
                } else {
                    assertThat(db.getInt(0), lessThanOrEqualTo(max));
                }
            }

            @Override
            public AggregatorFunction newAggregatorFunction(DriverContext context) {
                return new MaxIntAggregatorFunctionSupplier().aggregator(context, List.of(0, 1));
            }

            @Override
            public void assertMaxValue(Block block, boolean exactResult) {
                assertThat(block, instanceOf(IntBlock.class));
                final IntBlock ib = (IntBlock) block;
                if (exactResult) {
                    assertThat(ib.getInt(0), equalTo(max));
                } else {
                    assertThat(ib.getInt(0), lessThanOrEqualTo(max));
                }
            }
        };
    }
}
