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

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.Stash;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class StashTests extends ESTestCase {
    public void testReplaceStashedValuesEmbeddedStashKey() throws IOException {
        Stash stash = new Stash();
        stash.stashValue("stashed", "bar");
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("key", singletonMap("a", "foobar"));
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        map2.put("a", "foo${stashed}");
        map.put("key", map2);

        Map<String, Object> actual = stash.replaceStashedValues(map);
        assertEquals(expected, actual);
    }
}
