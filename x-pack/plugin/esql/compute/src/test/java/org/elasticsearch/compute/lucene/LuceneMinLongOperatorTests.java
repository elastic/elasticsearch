/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.MinLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneMinLongOperatorTests extends LuceneMinOperatorTestCase {

    @Override
    public LuceneMinFactory.NumberType getNumberType() {
        return LuceneMinFactory.NumberType.LONG;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            long min = Long.MAX_VALUE;

            @Override
            public IndexableField newPointField() {
                return new LongField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, newValue());
            }

            private long newValue() {
                final long value = randomLong();
                min = Math.min(min, value);
                return value;
            }

            @Override
            public void assertPage(Page page) {
                assertThat(page.getBlock(0), instanceOf(LongBlock.class));
                final LongBlock db = page.getBlock(0);
                assertThat(page.getBlock(1), instanceOf(BooleanBlock.class));
                final BooleanBlock bb = page.getBlock(1);
                if (bb.getBoolean(0) == false) {
                    assertThat(db.getLong(0), equalTo(Long.MAX_VALUE));
                } else {
                    assertThat(db.getLong(0), greaterThanOrEqualTo(min));
                }
            }

            @Override
            public AggregatorFunction newAggregatorFunction(DriverContext context) {
                return new MinLongAggregatorFunctionSupplier().aggregator(context, List.of(0, 1));
            }

            @Override
            public void assertMinValue(Block block, boolean exactResult) {
                assertThat(block, instanceOf(LongBlock.class));
                final LongBlock lb = (LongBlock) block;
                if (exactResult) {
                    assertThat(lb.getLong(0), equalTo(min));
                } else {
                    assertThat(lb.getLong(0), greaterThanOrEqualTo(min));
                }
            }
        };
    }
}
