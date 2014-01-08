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

package org.elasticsearch.common.collect;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Iterator;
import java.util.List;

public class Iterators2Tests extends ElasticsearchTestCase {

    public void testDeduplicateSorted() {
        final List<String> list = Lists.newArrayList();
        for (int i = randomInt(100); i >= 0; --i) {
            final int frequency = randomIntBetween(1, 10);
            final String s = randomAsciiOfLength(randomIntBetween(2, 20));
            for (int j = 0; j < frequency; ++j) {
                list.add(s);
            }
        }
        CollectionUtil.introSort(list);
        final List<String> deduplicated = Lists.newArrayList();
        for (Iterator<String> it = Iterators2.deduplicateSorted(list.iterator(), Ordering.natural()); it.hasNext(); ) {
            deduplicated.add(it.next());
        }
        assertEquals(Lists.newArrayList(Sets.newTreeSet(list)), deduplicated);
    }

}
