/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LuceneMaxDoubleOperatorTests extends LuceneMaxOperatorTestCase {

    @Override
    public LuceneMaxFactory.NumberType getNumberType() {
        return LuceneMaxFactory.NumberType.DOUBLE;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            double max = -Double.MAX_VALUE;

            @Override
            public IndexableField newPointField() {
                return new DoubleField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(newValue()));
            }

            private double newValue() {
                final double value = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
                max = Math.max(max, value);
                return value;
            }

            @Override
            public void assertPage(Page page) {
                assertThat(page.getBlock(0), instanceOf(DoubleBlock.class));
                final DoubleBlock db = page.getBlock(0);
                assertThat(page.getBlock(1), instanceOf(BooleanBlock.class));
                final BooleanBlock bb = page.getBlock(1);
                if (bb.getBoolean(0) == false) {
                    assertThat(db.getDouble(0), equalTo(-Double.MAX_VALUE));
                } else {
                    assertThat(db.getDouble(0), lessThanOrEqualTo(max));
                }
            }

            @Override
            public AggregatorFunction newAggregatorFunction(DriverContext context) {
                return new MaxDoubleAggregatorFunctionSupplier(List.of(0, 1)).aggregator(context);
            }

            @Override
            public void assertMaxValue(Block block, boolean exactResult) {
                assertThat(block, instanceOf(DoubleBlock.class));
                final DoubleBlock db = (DoubleBlock) block;
                if (exactResult) {
                    assertThat(db.getDouble(0), equalTo(max));
                } else {
                    assertThat(db.getDouble(0), lessThanOrEqualTo(max));
                }
            }
        };
    }
}
