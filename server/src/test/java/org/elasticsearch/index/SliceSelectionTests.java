/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SliceSelectionTests extends ESTestCase {

    public void testUnspecified() {
        SliceSelection slices = SliceSelection.fromSearchSlice(null);
        assertThat(slices, sameInstance(SliceSelection.UNSPECIFIED));
        assertFalse(slices.isSpecified());
        assertFalse(slices.isRestricted());
        assertThat(slices.names(), empty());
        assertNull(slices.toSearchSlice());
        assertNull(slices.toRouting());
    }

    public void testAll() {
        SliceSelection slices = SliceSelection.fromSearchSlice(SliceIndexing.SLICE_ALL);
        assertThat(slices, sameInstance(SliceSelection.ALL));
        assertTrue(slices.isSpecified());
        assertFalse(slices.isRestricted());
        assertThat(slices.names(), empty());
        assertThat(slices.toSearchSlice(), equalTo(SliceIndexing.SLICE_ALL));
        assertNull(slices.toRouting());
    }

    public void testNamed() {
        SliceSelection slices = SliceSelection.fromSearchSlice("s1,s2");
        assertThat(slices.kind(), equalTo(SliceSelection.Kind.NAMED));
        assertTrue(slices.isSpecified());
        assertTrue(slices.isRestricted());
        assertThat(slices.names(), contains("s1", "s2"));
        assertThat(slices.toSearchSlice(), equalTo("s1,s2"));
        assertThat(slices.toRouting(), equalTo("s1,s2"));
        assertThat(SliceSelection.fromSearchSlice(slices.toSearchSlice()), equalTo(slices));
    }

    public void testNamesAreTrimmedAndDeduplicated() {
        assertThat(SliceSelection.fromSearchSlice(" s1 ,s2,s1").names(), contains("s1", "s2"));
        assertThat(SliceSelection.of(List.of("s2", "s1", "s2")).names(), contains("s2", "s1"));
    }

    public void testRejectsBlankNames() {
        for (String blank : List.of("", "   ", "s1,,s2", "s1, ")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SliceSelection.fromSearchSlice(blank));
            assertThat(e.getMessage(), containsString("[_slice] cannot be blank"));
        }
        expectThrows(IllegalArgumentException.class, () -> SliceSelection.of(List.of()));
    }

    public void testRejectsAllCombinedWithNames() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SliceSelection.fromSearchSlice("s1,_all"));
        assertThat(e.getMessage(), containsString("[_all] cannot be combined with other slices"));
        expectThrows(IllegalArgumentException.class, () -> SliceSelection.of(List.of(SliceIndexing.SLICE_ALL)));
    }

    public void testNamesRequireNamedKind() {
        expectThrows(IllegalArgumentException.class, () -> new SliceSelection(SliceSelection.Kind.ALL, List.of("s1")));
        expectThrows(IllegalArgumentException.class, () -> new SliceSelection(SliceSelection.Kind.NAMED, List.of()));
    }
}
