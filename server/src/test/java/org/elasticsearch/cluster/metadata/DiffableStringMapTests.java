/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
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

    public void testRandomDiffing() {
        Map<String, String> m = new HashMap<>();
        m.put("1", "1");
        m.put("2", "2");
        m.put("3", "3");
        DiffableStringMap dsm = new DiffableStringMap(m);
        Map<String, String> expected = new HashMap<>(m);

        for (int i = 0; i < randomIntBetween(5, 50); i++) {
            if (randomBoolean() && expected.size() > 1) {
                expected.remove(randomFrom(expected.keySet()));
            } else if (randomBoolean()) {
                expected.put(randomFrom(expected.keySet()), randomAlphaOfLength(4));
            } else {
                expected.put(randomAlphaOfLength(2), randomAlphaOfLength(4));
            }
            dsm = new DiffableStringMap(expected).diff(dsm).apply(dsm);
        }
        assertThat(expected, equalTo(dsm));
    }

    public void testSerialization() throws IOException {
        Map<String, String> m = new HashMap<>();
        // Occasionally have an empty map
        if (frequently()) {
            m.put("foo", "bar");
            m.put("baz", "eggplant");
            m.put("potato", "canon");
        }
        DiffableStringMap dsm = new DiffableStringMap(m);

        BytesStreamOutput bso = new BytesStreamOutput();
        dsm.writeTo(bso);
        DiffableStringMap deserialized = DiffableStringMap.readFrom(bso.bytes().streamInput());
        assertThat(deserialized, equalTo(dsm));
    }
}
