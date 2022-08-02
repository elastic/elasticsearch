/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dissect;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class DissectMatchTests extends ESTestCase {

    public void testIllegalArgs() {
        expectThrows(IllegalArgumentException.class, () -> new DissectMatch("", 0, 1, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new DissectMatch("", 1, 0, 0, 0));
    }

    public void testValidAndFullyMatched() {
        int expectedMatches = randomIntBetween(1, 26);
        DissectMatch dissectMatch = new DissectMatch("", expectedMatches, expectedMatches, 0, 0);
        IntStream.range(97, 97 + expectedMatches)  // allow for a-z values
            .forEach(i -> dissectMatch.add(new DissectKey(new String(new byte[] { (byte) i }, StandardCharsets.UTF_8)), ""));
        assertThat(dissectMatch.fullyMatched(), equalTo(true));
        assertThat(dissectMatch.isValid(dissectMatch.getResults()), equalTo(true));
    }

    public void testNotValidAndFullyMatched() {
        int expectedMatches = randomIntBetween(1, 26);
        DissectMatch dissectMatch = new DissectMatch("", expectedMatches, expectedMatches, 0, 0);
        IntStream.range(97, 97 + expectedMatches - 1)  // allow for a-z values
            .forEach(i -> dissectMatch.add(new DissectKey(new String(new byte[] { (byte) i }, StandardCharsets.UTF_8)), ""));
        assertThat(dissectMatch.fullyMatched(), equalTo(false));
        assertThat(dissectMatch.isValid(dissectMatch.getResults()), equalTo(false));
    }

    public void testGetResultsIdempotent() {
        int expectedMatches = randomIntBetween(1, 26);
        DissectMatch dissectMatch = new DissectMatch("", expectedMatches, expectedMatches, 0, 0);
        IntStream.range(97, 97 + expectedMatches)  // allow for a-z values
            .forEach(i -> dissectMatch.add(new DissectKey(new String(new byte[] { (byte) i }, StandardCharsets.UTF_8)), ""));
        assertThat(dissectMatch.getResults(), equalTo(dissectMatch.getResults()));
    }

    public void testAppend() {
        DissectMatch dissectMatch = new DissectMatch("-", 3, 1, 3, 0);
        dissectMatch.add(new DissectKey("+a"), "x");
        dissectMatch.add(new DissectKey("+a"), "y");
        dissectMatch.add(new DissectKey("+a"), "z");
        Map<String, String> results = dissectMatch.getResults();
        assertThat(dissectMatch.isValid(results), equalTo(true));
        assertThat(results, equalTo(MapBuilder.newMapBuilder().put("a", "x-y-z").map()));
    }

    public void testAppendWithOrder() {
        DissectMatch dissectMatch = new DissectMatch("-", 3, 1, 3, 0);
        dissectMatch.add(new DissectKey("+a/3"), "x");
        dissectMatch.add(new DissectKey("+a"), "y");
        dissectMatch.add(new DissectKey("+a/1"), "z");
        Map<String, String> results = dissectMatch.getResults();
        assertThat(dissectMatch.isValid(results), equalTo(true));
        assertThat(results, equalTo(MapBuilder.newMapBuilder().put("a", "y-z-x").map()));
    }

    public void testReference() {
        DissectMatch dissectMatch = new DissectMatch("-", 2, 1, 0, 1);
        dissectMatch.add(new DissectKey("&a"), "x");
        dissectMatch.add(new DissectKey("*a"), "y");
        Map<String, String> results = dissectMatch.getResults();
        assertThat(dissectMatch.isValid(results), equalTo(true));
        assertThat(results, equalTo(MapBuilder.newMapBuilder().put("y", "x").map()));
    }

}
