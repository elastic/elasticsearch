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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntBlock;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneMaxIntOperatorTests extends LuceneMaxOperatorTestCase {

    @Override
    public LuceneMaxFactory.NumberType getNumberType() {
        return LuceneMaxFactory.NumberType.INTEGER;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            int max = Integer.MIN_VALUE;
            int result = Integer.MIN_VALUE;

            @Override
            public IndexableField newPointField() {
                return new IntField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            private int newValue() {
                int value = randomInt();
                max = Math.max(max, value);
                return value;
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, newValue());
            }

            @Override
            public void assertPage(Block block, BooleanBlock bb) {
                assertThat(block, instanceOf(IntBlock.class));
                IntBlock ib = (IntBlock) block;
                int v = ib.getInt(0);
                result = Math.max(result, v);
                if (bb.getBoolean(0) == false) {
                    assertThat(v, equalTo(Integer.MIN_VALUE));
                }
            }

            @Override
            public void assertMaxValue() {
                assertThat(result, equalTo(max));
            }
        };
    }
}
