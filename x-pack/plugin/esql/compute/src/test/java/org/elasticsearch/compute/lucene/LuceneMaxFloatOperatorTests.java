/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LuceneMaxFloatOperatorTests extends LuceneMaxOperatorTestCase {

    @Override
    public LuceneMaxFactory.NumberType getNumberType() {
        return LuceneMaxFactory.NumberType.FLOAT;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            float max = -Float.MAX_VALUE;

            @Override
            public IndexableField newPointField() {
                return new FloatField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            private float newValue() {
                final float value = randomFloatBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
                max = Math.max(max, value);
                return value;
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.floatToSortableInt(newValue()));
            }

            @Override
            public void assertPage(Page page) {
                assertThat(page.getBlock(0), instanceOf(FloatBlock.class));
                final FloatBlock db = page.getBlock(0);
                assertThat(page.getBlock(1), instanceOf(BooleanBlock.class));
                final BooleanBlock bb = page.getBlock(1);
                if (bb.getBoolean(0) == false) {
                    assertThat(db.getFloat(0), equalTo(-Float.MAX_VALUE));
                } else {
                    assertThat(db.getFloat(0), lessThanOrEqualTo(max));
                }
            }

            @Override
            public AggregatorFunction newAggregatorFunction(DriverContext context) {
                return new MaxFloatAggregatorFunctionSupplier().aggregator(context, List.of(0, 1));
            }

            @Override
            public void assertMaxValue(Block block, boolean exactResult) {
                assertThat(block, instanceOf(FloatBlock.class));
                final FloatBlock fb = (FloatBlock) block;
                if (exactResult) {
                    assertThat(fb.getFloat(0), equalTo(max));
                } else {
                    assertThat(fb.getFloat(0), lessThanOrEqualTo(max));
                }
            }
        };
    }
}
