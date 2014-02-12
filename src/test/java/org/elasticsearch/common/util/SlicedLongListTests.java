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

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link SlicedLongList}
 */
public class SlicedLongListTests extends ElasticsearchTestCase {

    @Test
    public void testCapacity() {
        SlicedLongList list = new SlicedLongList(5);
        assertThat(list.length, equalTo(5));
        assertThat(list.offset, equalTo(0));
        assertThat(list.values.length, equalTo(5));
        assertThat(list.size(), equalTo(5));
        
        list = new SlicedLongList(new long[10], 5, 5);
        assertThat(list.length, equalTo(5));
        assertThat(list.offset, equalTo(5));
        assertThat(list.size(), equalTo(5));
        assertThat(list.values.length, equalTo(10));
    }
    
    @Test
    public void testGrow() {
        SlicedLongList list = new SlicedLongList(5);
        list.length = 1000;
        for (int i = 0; i < list.length; i++) {
            list.grow(i+1);
            list.values[i] = ((long)i);
        }
        int expected = 0;
        for (Long d : list) {
            assertThat((long)expected++, equalTo(d));
        }
        
        for (int i = 0; i < list.length; i++) {
            assertThat((long)i, equalTo(list.get(i)));
        }
        
        int count = 0;
        for (int i = list.offset; i < list.offset+list.length; i++) {
            assertThat((long)count++, equalTo(list.values[i]));
        }
    }
    
    @Test
    public void testSet() {
        SlicedLongList list = new SlicedLongList(5);
        try {
            list.set(0, (long)4);
            fail();
        } catch (UnsupportedOperationException ex) {
        }
        try {
            list.add((long)4);
            fail();
        } catch (UnsupportedOperationException ex) {
        }
    }
    
    @Test
    public void testIndexOf() {
        SlicedLongList list = new SlicedLongList(5);
        list.length = 1000;
        for (int i = 0; i < list.length; i++) {
            list.grow(i+1);
            list.values[i] = ((long)i%100);
        }
        
        assertThat(999, equalTo(list.lastIndexOf(99l)));
        assertThat(99, equalTo(list.indexOf(99l)));
        
        assertThat(-1, equalTo(list.lastIndexOf(100l)));
        assertThat(-1, equalTo(list.indexOf(100l)));
    }
    
    public void testIsEmpty() {
        SlicedLongList list = new SlicedLongList(5);
        assertThat(false, equalTo(list.isEmpty()));
        list.length = 0;
        assertThat(true, equalTo(list.isEmpty()));
    }
    
    @Test
    public void testToString() {
        SlicedLongList list = new SlicedLongList(5);
        assertThat("[0, 0, 0, 0, 0]", equalTo(list.toString()));
        for (int i = 0; i < list.length; i++) {
            list.grow(i+1);
            list.values[i] = ((long)i);
        }
        assertThat("[0, 1, 2, 3, 4]", equalTo(list.toString()));
    }
    
}
