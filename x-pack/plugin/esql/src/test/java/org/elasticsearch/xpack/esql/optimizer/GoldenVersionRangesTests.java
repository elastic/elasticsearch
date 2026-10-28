/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase.Label;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase.VersionRange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/** Pins down {@link GoldenTestCase#deriveRanges}: how declared cut-points partition the released versions into ranges. */
public class GoldenVersionRangesTests extends ESTestCase {

    private static TransportVersion v(int id) {
        return new TransportVersion(id);
    }

    public void testNoLabelsIsOneFlatRange() {
        List<VersionRange> ranges = GoldenTestCase.deriveRanges(v(1000), List.of(), List.of(v(500), v(1000), v(2000), v(3000)));
        assertThat(ranges, hasSize(1));
        assertThat(ranges.getFirst().dir(), nullValue());
        assertThat(ranges.getFirst().start(), equalTo(v(1000)));
        // 500 is below the lower bound
        assertThat(ranges.getFirst().versions(), contains(v(1000), v(2000), v(3000)));
    }

    public void testLabelsPartitionTheWindow() {
        List<VersionRange> ranges = GoldenTestCase.deriveRanges(
            v(1000),
            List.of(new Label("l1", v(3000)), new Label("l2", v(5000))),
            List.of(v(1000), v(2000), v(3000), v(4000), v(5000), v(6000))
        );
        assertThat(ranges, hasSize(3));
        assertThat(ranges.get(0).dir(), equalTo("before_l1"));
        assertThat(ranges.get(0).start(), equalTo(v(1000)));
        assertThat(ranges.get(0).versions(), contains(v(1000), v(2000)));
        assertThat(ranges.get(1).dir(), equalTo("before_l2"));
        assertThat(ranges.get(1).start(), equalTo(v(3000)));
        assertThat(ranges.get(1).versions(), contains(v(3000), v(4000)));
        // the newest range's files live directly in the test directory
        assertThat(ranges.get(2).dir(), nullValue());
        assertThat(ranges.get(2).start(), equalTo(v(5000)));
        assertThat(ranges.get(2).versions(), contains(v(5000), v(6000)));
    }

    public void testEveryVersionLandsInExactlyOneRange() {
        List<TransportVersion> released = new ArrayList<>();
        for (int id = 1000; id <= 20000; id += 1000) {
            released.add(v(id));
        }
        int lowerBound = randomIntBetween(1, 5) * 1000;
        List<Label> labels = new ArrayList<>();
        int cut = lowerBound;
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            cut += randomIntBetween(1, 4) * 1000;
            labels.add(new Label("l" + i, v(cut)));
        }
        List<VersionRange> ranges = GoldenTestCase.deriveRanges(v(lowerBound), labels, released);
        List<TransportVersion> sampled = ranges.stream().flatMap(r -> r.versions().stream()).toList();
        // without patch versions there are no straddlers: the union is exactly the window above the bound
        assertThat(sampled, equalTo(released.stream().filter(version -> version.id() >= lowerBound).toList()));
        for (int i = 0; i < ranges.size(); i++) {
            for (TransportVersion version : ranges.get(i).versions()) {
                assertTrue(version.supports(ranges.get(i).start()));
                if (i < ranges.size() - 1) {
                    assertFalse(version.supports(ranges.get(i + 1).start()));
                }
            }
        }
    }

    public void testSinceRaisesTheLowerBound() {
        List<VersionRange> ranges = GoldenTestCase.deriveRanges(v(3000), List.of(), List.of(v(1000), v(2000), v(3000), v(4000)));
        assertThat(ranges.getFirst().versions(), contains(v(3000), v(4000)));
    }

    public void testMisorderedLabelsThrow() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> GoldenTestCase.deriveRanges(
                v(1000),
                List.of(new Label("newer", v(5000)), new Label("older", v(3000))),
                List.of(v(1000), v(6000))
            )
        );
        assertThat(e.getMessage(), equalTo("expectationChangesAt labels must be declared oldest-first: [older] is out of order"));
    }

    public void testDuplicateLabelsThrow() {
        expectThrows(
            IllegalArgumentException.class,
            () -> GoldenTestCase.deriveRanges(v(1000), List.of(new Label("a", v(3000)), new Label("b", v(3000))), List.of(v(1000), v(6000)))
        );
    }

    public void testLabelBelowTheFloorLeavesBaseDead() {
        // the compatibility floor slid past the label: the base range must come back empty, not fail
        List<VersionRange> ranges = GoldenTestCase.deriveRanges(v(3000), List.of(new Label("old", v(2000))), List.of(v(3000), v(4000)));
        assertThat(ranges, hasSize(2));
        assertThat(ranges.get(0).versions(), empty());
        assertThat(ranges.get(1).versions(), contains(v(3000), v(4000)));
    }

    public void testBackportStraddlerFails() {
        // 2001 supports l2 through its backport patch but not l1: an interfering backport no range can describe
        TransportVersion l2 = new TransportVersion("l2", 4000, new TransportVersion(null, 2001, null));
        TransportVersion straddler = v(2001);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> GoldenTestCase.deriveRanges(
                v(1000),
                List.of(new Label("l1", v(3000)), new Label("l2", l2)),
                List.of(v(1000), straddler, v(3000), v(4000))
            )
        );
        assertThat(e.getMessage(), containsString("a version-aware change was backported below another one that wasn't"));
    }

    public void testSampledIsAscendingRegardlessOfInputOrder() {
        List<TransportVersion> released = new ArrayList<>(List.of(v(1000), v(2000), v(3000), v(4000)));
        Collections.shuffle(released, random());
        List<VersionRange> ranges = GoldenTestCase.deriveRanges(v(1000), List.of(), released);
        assertThat(ranges.getFirst().versions(), contains(v(1000), v(2000), v(3000), v(4000)));
    }
}
