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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.FloatBlock;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneMaxFloatOperatorTests extends LuceneMaxOperatorTestCase {

    @Override
    public LuceneMaxFactory.NumberType getNumberType() {
        return LuceneMaxFactory.NumberType.FLOAT;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            float max = -Float.MAX_VALUE;
            float result = -Float.MAX_VALUE;

            @Override
            public IndexableField newPointField() {
                return new FloatField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            private float newValue() {
                float value = randomFloatBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
                max = Math.max(max, value);
                return value;
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.floatToSortableInt(newValue()));
            }

            @Override
            public void assertPage(Block block, BooleanBlock bb) {
                assertThat(block, instanceOf(FloatBlock.class));
                FloatBlock fb = (FloatBlock) block;
                float v = fb.getFloat(0);
                result = Math.max(result, v);
                if (bb.getBoolean(0) == false) {
                    assertThat(v, equalTo(-Float.MAX_VALUE));
                }
            }

            @Override
            public void assertMaxValue() {
                assertThat(result, equalTo(max));
            }
        };
    }
}
