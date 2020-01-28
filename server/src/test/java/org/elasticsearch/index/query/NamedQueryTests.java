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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class NamedQueryTests extends ESTestCase {

    private static class InstanceMatches implements Matches {

        @Override
        public MatchesIterator getMatches(String field) {
            return null;
        }

        @Override
        public Collection<Matches> getSubMatches() {
            return Collections.emptyList();
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.emptyIterator();
        }
    }

    public void testFindNamedMatches() {
        Matches m1 = new InstanceMatches();
        Matches m2 = new InstanceMatches();
        Matches n2 = new NamedQuery.NamedMatches("m2", m2);
        Matches m3 = new InstanceMatches();
        Matches n3 = new NamedQuery.NamedMatches("m3", m3);
        Matches m4 = new InstanceMatches();

        Matches all = MatchesUtils.fromSubMatches(Arrays.asList(m1, n2, m4));
        all = MatchesUtils.fromSubMatches(Arrays.asList(all, n3));
        List<String> names = NamedQuery.findNamedMatches(all)
            .stream().map(NamedQuery.NamedMatches::getName).collect(Collectors.toList());

        assertThat(names, containsInAnyOrder("m2", "m3"));
    }
}
