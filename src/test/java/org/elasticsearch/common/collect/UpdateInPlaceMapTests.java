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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class UpdateInPlaceMapTests extends ElasticsearchTestCase {

    @Test
    public void testConcurrentMutator() {
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(100);
        UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
        try {
            map.mutator();
            fail("should fail on concurrent mutator");
        } catch (ElasticsearchIllegalStateException e) {
            // all is well!
        }
        mutator.close();
        // now this should work well!
        map.mutator();
    }

    @Test
    public void testImmutableMapState() {
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(100);
        UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
        mutator.put("key1", "value1");
        assertThat(mutator.get("key1"), equalTo("value1"));
        assertThat(map.get("key1"), nullValue());
        mutator.close();
        assertThat(map.get("key1"), equalTo("value1"));

        mutator = map.mutator();
        mutator.remove("key1");
        assertThat(mutator.get("key1"), nullValue());
        assertThat(map.get("key1"), equalTo("value1"));
        mutator.close();
        assertThat(map.get("key1"), nullValue());
    }

    @Test
    public void testSwitchToCHM() {
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(1);
        UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
        mutator.put("key1", "value1");
        assertThat(mutator.get("key1"), equalTo("value1"));
        mutator.put("key2", "value2");
        assertThat(mutator.get("key2"), equalTo("value2"));
        // now that we changed to CHM, things are visible on the map as well!
        assertThat(map.get("key1"), equalTo("value1"));
        assertThat(map.get("key2"), equalTo("value2"));
        mutator.close();
        assertThat(map.get("key1"), equalTo("value1"));
        assertThat(map.get("key2"), equalTo("value2"));

        mutator = map.mutator();
        mutator.remove("key1");
        assertThat(mutator.get("key1"), nullValue());
        assertThat(map.get("key1"), nullValue());
        mutator.close();
        assertThat(map.get("key1"), nullValue());
    }

    @Test
    public void testInitializeWithCHM() {
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(0);
        UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
        mutator.put("key1", "value1");
        assertThat(mutator.get("key1"), equalTo("value1"));
        mutator.put("key2", "value2");
        assertThat(mutator.get("key2"), equalTo("value2"));
    }
}
