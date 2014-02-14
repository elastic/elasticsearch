/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
/**
 * Tests for {@link SlicedObjectList}
 */
public class SlicedObjectListTests extends ElasticsearchTestCase {

    public class TestList extends SlicedObjectList<Double> {
        
        public TestList(int capactiy) {
            this(new Double[capactiy], 0, capactiy);
        }

        public TestList(Double[] values, int offset, int length) {
            super(values, offset, length);
        }

        public TestList(Double[] values) {
            super(values);
        }

        @Override
        public void grow(int newLength) {
            assertThat(offset, equalTo(0)); // NOTE: senseless if offset != 0
            if (values.length >= newLength) {
                return;
            }
            final Double[] current = values;
            values = new Double[ArrayUtil.oversize(newLength, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(current, 0, values, 0, current.length);
            
        }
        
    }
    @Test
    public void testCapacity() {
        TestList list = new TestList(5);
        assertThat(list.length, equalTo(5));
        assertThat(list.offset, equalTo(0));
        assertThat(list.values.length, equalTo(5));
        assertThat(list.size(), equalTo(5));

        
        list = new TestList(new Double[10], 5, 5);
        assertThat(list.length, equalTo(5));
        assertThat(list.offset, equalTo(5));
        assertThat(list.size(), equalTo(5));
        assertThat(list.values.length, equalTo(10));
    }
    
    @Test
    public void testGrow() {
        TestList list = new TestList(5);
        list.length = 1000;
        for (int i = 0; i < list.length; i++) {
            list.grow(i+1);
            list.values[i] = ((double)i);
        }
        int expected = 0;
        for (Double d : list) {
            assertThat((double)expected++, equalTo(d));
        }
        
        for (int i = 0; i < list.length; i++) {
            assertThat((double)i, equalTo(list.get(i)));
        }
        
        int count = 0;
        for (int i = list.offset; i < list.offset+list.length; i++) {
            assertThat((double)count++, equalTo(list.values[i]));
        }
    }
    
    @Test
    public void testIndexOf() {
        TestList list = new TestList(5);
        list.length = 1000;
        for (int i = 0; i < list.length; i++) {
            list.grow(i+1);
            list.values[i] = ((double)i%100);
        }
        
        assertThat(999, equalTo(list.lastIndexOf(99.0d)));
        assertThat(99, equalTo(list.indexOf(99.0d)));
        
        assertThat(-1, equalTo(list.lastIndexOf(100.0d)));
        assertThat(-1, equalTo(list.indexOf(100.0d)));
    }
    
    public void testIsEmpty() {
        TestList list = new TestList(5);
        assertThat(false, equalTo(list.isEmpty()));
        list.length = 0;
        assertThat(true, equalTo(list.isEmpty()));
    }
    
    @Test
    public void testSet() {
        TestList list = new TestList(5);
        try {
            list.set(0, (double)4);
            fail();
        } catch (UnsupportedOperationException ex) {
        }
        try {
            list.add((double)4);
            fail();
        } catch (UnsupportedOperationException ex) {
        }
    }
    
    @Test
    public void testToString() {
        TestList list = new TestList(5);
        assertThat("[null, null, null, null, null]", equalTo(list.toString()));
        for (int i = 0; i < list.length; i++) {
            list.grow(i+1);
            list.values[i] = ((double)i);
        }
        assertThat("[0.0, 1.0, 2.0, 3.0, 4.0]", equalTo(list.toString()));
    }
    
}
