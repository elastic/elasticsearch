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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DiffableStringMapTests extends ESTestCase {

    public void testDiffableStringMapDiff() {
        Map<String, String> m = new HashMap<>();
        m.put("foo", "bar");
        m.put("baz", "eggplant");
        m.put("potato", "canon");
        DiffableStringMap dsm = new DiffableStringMap(m);

        Map<String, String> m2 = new HashMap<>();
        m2.put("foo", "not-bar");
        m2.put("newkey", "yay");
        m2.put("baz", "eggplant");
        DiffableStringMap dsm2 = new DiffableStringMap(m2);

        Diff<DiffableStringMap> diff = dsm2.diff(dsm);
        assertThat(diff, instanceOf(DiffableStringMap.DiffableStringMapDiff.class));
        DiffableStringMap.DiffableStringMapDiff dsmd = (DiffableStringMap.DiffableStringMapDiff) diff;

        assertThat(dsmd.getDeletes(), containsInAnyOrder("potato"));
        assertThat(dsmd.getDiffs().size(), equalTo(0));
        Map<String, String> upserts = new HashMap<>();
        upserts.put("foo", "not-bar");
        upserts.put("newkey", "yay");
        assertThat(dsmd.getUpserts(), equalTo(upserts));

        DiffableStringMap dsm3 = diff.apply(dsm);
        assertThat(dsm3.get("foo"), equalTo("not-bar"));
        assertThat(dsm3.get("newkey"), equalTo("yay"));
        assertThat(dsm3.get("baz"), equalTo("eggplant"));
        assertThat(dsm3.get("potato"), equalTo(null));
    }
}
