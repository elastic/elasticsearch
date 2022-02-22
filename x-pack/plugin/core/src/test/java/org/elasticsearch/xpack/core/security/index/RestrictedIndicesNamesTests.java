/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.index;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.is;

public class RestrictedIndicesNamesTests extends ESTestCase {

    private final CharacterRunAutomaton RUN_AUTOMATON = new CharacterRunAutomaton(RestrictedIndicesNames.NAMES_AUTOMATON);

    public void testAsyncSearchNames() {
        testIndex(".async-search", true);
        testIndex(".async-search" + (randomBoolean() ? "-" : "") + randomAlphaOfLengthBetween(1, 8), true);
        testIndex(".async-search" + (randomBoolean() ? "-" : "") + randomInt(), true);
        testIndex("async-search" + (randomBoolean() ? "-" : "") + randomInt(), false);
        testIndex(".asynchronous-search" + (randomBoolean() ? "-" : "") + randomInt(), false);
        testIndex(".not-async-search" + (randomBoolean() ? "-" : "") + randomAlphaOfLengthBetween(1, 8), false);
    }

    public void testSecurityNames() {
        testIndex(".security", true);
        testIndex(".security-6", true);
        testIndex(".security-7", true);
        testIndex(".security-" + randomIntBetween(0, 999_999), true);
        testIndex(".security-" + randomIntBetween(0, 99) + (randomBoolean() ? "-" : "") + randomAlphaOfLengthBetween(1, 20), true);

        testIndex(".security-tokens-7", true);
        testIndex(".security-tokens-" + randomIntBetween(0, 99) + (randomBoolean() ? "-" : "") + randomAlphaOfLengthBetween(1, 20), true);

        testIndex("security", false);
        testIndex(randomAlphaOfLength(1) + "security", false);
        testIndex("security-6", false);
        testIndex("@security-6", false);
        testIndex(".not-security-7", false);
        testIndex(".security-", false);
        testIndex("security-tokens", false);
        testIndex("security-tokens-7", false);
        testIndex("_security-tokens", false);
        testIndex(".security-" + randomAlphaOfLengthBetween(1, 3), false);
        testIndex(".security-tokens-" + randomAlphaOfLengthBetween(1, 3), false);
        testIndex(".security" + randomAlphaOfLengthBetween(1, 10), false);
        testIndex(".security-tokens" + randomAlphaOfLengthBetween(1, 10), false);
        testIndex(".security" + randomIntBetween(1, 9), false);
        testIndex(".security-" + randomAlphaOfLength(1) + randomIntBetween(1, 9), false);
        testIndex(".security" + randomAlphaOfLength(1) + randomIntBetween(1, 9), false);
    }

    private void testIndex(String name, boolean expected) {
        assertThat("For index [" + name + "]", RestrictedIndicesNames.isRestricted(name), is(expected));
        assertThat("For index [" + name + "]", RUN_AUTOMATON.run(name), is(expected));
    }

}
