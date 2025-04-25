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
import org.elasticsearch.compute.aggregation.MinDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneMinDoubleOperatorTests extends LuceneMinOperatorTestCase {

    @Override
    public LuceneMinFactory.NumberType getNumberType() {
        return LuceneMinFactory.NumberType.DOUBLE;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            double min = Double.MAX_VALUE;

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
                min = Math.min(min, value);
                return value;
            }

            @Override
            public void assertPage(Page page) {
                assertThat(page.getBlock(0), instanceOf(DoubleBlock.class));
                final DoubleBlock db = page.getBlock(0);
                assertThat(page.getBlock(1), instanceOf(BooleanBlock.class));
                final BooleanBlock bb = page.getBlock(1);
                if (bb.getBoolean(0) == false) {
                    assertThat(db.getDouble(0), equalTo(Double.POSITIVE_INFINITY));
                } else {
                    assertThat(db.getDouble(0), greaterThanOrEqualTo(min));
                }
            }

            @Override
            public AggregatorFunction newAggregatorFunction(DriverContext context) {
                return new MinDoubleAggregatorFunctionSupplier().aggregator(context, List.of(0, 1));
            }

            @Override
            public void assertMinValue(Block block, boolean exactResult) {
                assertThat(block, instanceOf(DoubleBlock.class));
                final DoubleBlock db = (DoubleBlock) block;
                if (exactResult) {
                    assertThat(db.getDouble(0), equalTo(min));
                } else {
                    assertThat(db.getDouble(0), greaterThanOrEqualTo(min));
                }
            }
        };
    }
}
