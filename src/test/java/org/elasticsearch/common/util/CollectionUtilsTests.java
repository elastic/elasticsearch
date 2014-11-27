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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CollectionUtilsTests extends ElasticsearchTestCase {

    @Test
    public void rotateEmpty() {
        assertTrue(CollectionUtils.rotate(ImmutableList.of(), randomInt()).isEmpty());
    }

    @Test
    public void rotate() {
        final int iters = scaledRandomIntBetween(10, 100);
        for (int k = 0; k < iters; ++k) {
            final int size = randomIntBetween(1, 100);
            final int distance = randomInt();
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                list.add(new Object());
            }
            final List<Object> rotated = CollectionUtils.rotate(list, distance);
            // check content is the same
            assertEquals(rotated.size(), list.size());
            assertEquals(Iterables.size(rotated), list.size());
            assertEquals(new HashSet<>(rotated), new HashSet<>(list));
            // check stability
            for (int j = randomInt(4); j >= 0; --j) {
                assertEquals(rotated, CollectionUtils.rotate(list, distance));
            }
            // reverse
            if (distance != Integer.MIN_VALUE) {
                assertEquals(list, CollectionUtils.rotate(CollectionUtils.rotate(list, distance), -distance));
            }
        }
    }

    @Test
    public void testSortAndDedupByteRefArray() {
        SortedSet<BytesRef> set = new TreeSet<>();
        final int numValues = scaledRandomIntBetween(0, 10000);
        List<BytesRef> tmpList = new ArrayList<>();
        BytesRefArray array = new BytesRefArray(Counter.newCounter());
        for (int i = 0; i < numValues; i++) {
            String s = randomRealisticUnicodeOfCodepointLengthBetween(1, 100);
            set.add(new BytesRef(s));
            tmpList.add(new BytesRef(s));
            array.append(new BytesRef(s));
        }
        if (randomBoolean()) {
            Collections.shuffle(tmpList, getRandom());
            for (BytesRef ref : tmpList) {
                array.append(ref);
            }
        }
        int[] indices = new int[array.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        int numUnique = CollectionUtils.sortAndDedup(array, indices);
        assertThat(numUnique, equalTo(set.size()));
        Iterator<BytesRef> iterator = set.iterator();

        BytesRefBuilder spare = new BytesRefBuilder();
        for (int i = 0; i < numUnique; i++) {
            assertThat(iterator.hasNext(), is(true));
            assertThat(array.get(spare, indices[i]), equalTo(iterator.next()));
        }

    }

    @Test
    public void testSortByteRefArray() {
        List<BytesRef> values = new ArrayList<>();
        final int numValues = scaledRandomIntBetween(0, 10000);
        BytesRefArray array = new BytesRefArray(Counter.newCounter());
        for (int i = 0; i < numValues; i++) {
            String s = randomRealisticUnicodeOfCodepointLengthBetween(1, 100);
            values.add(new BytesRef(s));
            array.append(new BytesRef(s));
        }
        if (randomBoolean()) {
            Collections.shuffle(values, getRandom());
        }
        int[] indices = new int[array.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        CollectionUtils.sort(array, indices);
        Collections.sort(values);
        Iterator<BytesRef> iterator = values.iterator();

        BytesRefBuilder spare = new BytesRefBuilder();
        for (int i = 0; i < values.size(); i++) {
            assertThat(iterator.hasNext(), is(true));
            assertThat(array.get(spare, indices[i]), equalTo(iterator.next()));
        }

    }

}
