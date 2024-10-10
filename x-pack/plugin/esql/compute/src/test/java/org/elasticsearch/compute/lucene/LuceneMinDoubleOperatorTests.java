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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;

import static org.hamcrest.Matchers.equalTo;
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
            double result = Double.MAX_VALUE;

            @Override
            public IndexableField newPointField() {
                return new DoubleField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(newValue()));
            }

            private double newValue() {
                double value = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
                min = Math.min(min, value);
                return value;
            }

            @Override
            public void assertPage(Block block, BooleanBlock bb) {
                assertThat(block, instanceOf(DoubleBlock.class));
                DoubleBlock db = (DoubleBlock) block;
                double v = db.getDouble(0);
                result = Math.min(result, v);
                if (bb.getBoolean(0) == false) {
                    assertThat(v, equalTo(Double.POSITIVE_INFINITY));
                }
            }

            @Override
            public void assertMaxValue() {
                assertThat(result, equalTo(min));
            }
        };
    }
}
