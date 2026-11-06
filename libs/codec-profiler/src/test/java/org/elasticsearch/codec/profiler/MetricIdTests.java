/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MetricIdTests extends ESTestCase {

    public void testAKeyRoundTrips() {
        for (int i = 0; i < 100; i++) {
            final Metric.Id id = randomId();
            assertThat(Metric.Id.parse(id.key()), equalTo(id));
            assertThat(id.key(), equalTo(id.group() + "." + id.component() + "." + id.name()));
        }
    }

    public void testOnlyAValidIdentityIsAccepted() {
        for (String key : List.of(
            "",
            "afc",
            "ColumNAR.read",
            "ColumNAR..read",
            "ColumNAR.blocks.",
            ".blocks.read",
            "ColumNAR.blocks.read.more",
            "ColumNAR.Blocks.read",
            "ColumNAR.blocks.Read",
            "1codec.blocks.read",
            "Colum-NAR.blocks.read",
            "ColumNAR.blocks.read time"
        )) {
            expectThrows(IllegalArgumentException.class, () -> Metric.Id.parse(key));
        }
        expectThrows(IllegalArgumentException.class, () -> new Metric.Id("ColumNAR", "blocks", "read.more"));
        expectThrows(IllegalArgumentException.class, () -> new Metric.Id("ColumNAR", "blocks.x", "read"));
        expectThrows(NullPointerException.class, () -> new Metric.Id(null, "blocks", "read"));
    }

    public void testIdentitiesAreOrderedLikeTheirKeys() {
        final List<Metric.Id> ids = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            ids.add(randomId());
        }
        final List<Metric.Id> byId = new ArrayList<>(ids);
        byId.sort(Comparator.naturalOrder());
        final List<Metric.Id> byKey = new ArrayList<>(ids);
        byKey.sort(Comparator.comparing(Metric.Id::key));
        assertThat(byId, equalTo(byKey));
    }

    public void testAPrefixOfAPartSortsFirstAsItsKeyDoes() {
        final Metric.Id shorter = new Metric.Id("A", "blocks", "read");
        final Metric.Id longer = new Metric.Id("A", "blocks_x", "a");
        final Metric.Id longerGroup = new Metric.Id("AB", "a", "a");
        assertTrue(shorter.compareTo(longer) < 0);
        assertTrue(shorter.key().compareTo(longer.key()) < 0);
        assertTrue(shorter.compareTo(longerGroup) < 0);
        assertTrue(shorter.key().compareTo(longerGroup.key()) < 0);
    }

    private static Metric.Id randomId() {
        return new Metric.Id(
            randomFrom("C", "ColumNAR", "Col", "c", "codec_2", "Numeric"),
            randomFrom("a", "blocks", "blocks_x", "b2", "pages", "z"),
            randomFrom("a", "read", "read_x", "r2", "count")
        );
    }
}
