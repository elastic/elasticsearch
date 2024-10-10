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

public class LuceneMinIntegerOperatorTests extends LuceneMinOperatorTestCase {

    @Override
    public LuceneMinFactory.NumberType getNumberType() {
        return LuceneMinFactory.NumberType.INTEGER;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            int min = Integer.MAX_VALUE;
            int result = Integer.MAX_VALUE;

            @Override
            public IndexableField newPointField() {
                return new IntField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, newValue());
            }

            private int newValue() {
                int value = randomInt();
                min = Math.min(min, value);
                return value;
            }

            @Override
            public void assertPage(Block block, BooleanBlock bb) {
                assertThat(block, instanceOf(IntBlock.class));
                IntBlock ib = (IntBlock) block;
                final int v = ib.getInt(0);
                result = Math.min(result, v);
                if (bb.getBoolean(0) == false) {
                    assertThat(v, equalTo(Integer.MAX_VALUE));
                }
            }

            @Override
            public void assertMaxValue() {
                assertThat(result, equalTo(min));
            }
        };
    }
}
