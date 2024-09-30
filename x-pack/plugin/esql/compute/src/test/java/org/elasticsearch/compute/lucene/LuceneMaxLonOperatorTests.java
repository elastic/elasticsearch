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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneMaxLonOperatorTests extends LuceneMaxOperatorTestCase {

    @Override
    public LuceneMaxFactory.NumberType getNumberType() {
        return LuceneMaxFactory.NumberType.LONG;
    }

    @Override
    protected NumberTypeTest getNumberTypeTest() {
        return new NumberTypeTest() {

            long max = Long.MIN_VALUE;
            long result = Long.MIN_VALUE;

            @Override
            public IndexableField newPointField() {
                return new LongField(FIELD_NAME, newValue(), randomFrom(Field.Store.values()));
            }

            @Override
            public IndexableField newDocValuesField() {
                return new SortedNumericDocValuesField(FIELD_NAME, newValue());
            }

            private long newValue() {
                long value = randomLong();
                max = Math.max(max, value);
                return value;
            }

            @Override
            public void assertPage(Block block, BooleanBlock bb) {
                assertThat(block, instanceOf(LongBlock.class));
                LongBlock lb = (LongBlock) block;
                long v = lb.getLong(0);
                result = Math.max(result, v);
                if (bb.getBoolean(0) == false) {
                    assertThat(v, equalTo(Long.MIN_VALUE));
                }
            }

            @Override
            public void assertMaxValue() {
                assertThat(result, equalTo(max));
            }
        };
    }
}
