/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dissect;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DissectKeyTests extends ESTestCase {

    public void testNoModifier() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        DissectKey dissectKey = new DissectKey(keyName);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.NONE));
        assertThat(dissectKey.skip(), is(false));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testAppendModifier() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        DissectKey dissectKey = new DissectKey("+" + keyName);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.APPEND));
        assertThat(dissectKey.skip(), is(false));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testAppendWithOrderModifier() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        int length = randomIntBetween(1, 100);
        DissectKey dissectKey = new DissectKey("+" + keyName + "/" + length);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.APPEND_WITH_ORDER));
        assertThat(dissectKey.skip(), is(false));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(length));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testAppendWithOrderModifierNoName() {
        int length = randomIntBetween(1, 100);
        DissectException e = expectThrows(DissectException.class, () -> new DissectKey("+/" + length));
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to parse key"));
    }

    public void testOrderModifierWithoutAppend() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        int length = randomIntBetween(1, 100);
        DissectException e = expectThrows(DissectException.class, () -> new DissectKey(keyName + "/" + length));
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to parse key"));
    }

    public void testFieldNameModifier() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        DissectKey dissectKey = new DissectKey("*" + keyName);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.FIELD_NAME));
        assertThat(dissectKey.skip(), is(false));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testFieldValueModifiers() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        DissectKey dissectKey = new DissectKey("&" + keyName);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.FIELD_VALUE));
        assertThat(dissectKey.skip(), is(false));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testRightPaddingModifiers() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        DissectKey dissectKey = new DissectKey(keyName + "->");
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.NONE));
        assertThat(dissectKey.skip(), is(false));
        assertThat(dissectKey.skipRightPadding(), is(true));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));

        dissectKey = new DissectKey("*" + keyName + "->");
        assertThat(dissectKey.skipRightPadding(), is(true));

        dissectKey = new DissectKey("&" + keyName + "->");
        assertThat(dissectKey.skipRightPadding(), is(true));

        dissectKey = new DissectKey("+" + keyName + "->");
        assertThat(dissectKey.skipRightPadding(), is(true));

        dissectKey = new DissectKey("?" + keyName + "->");
        assertThat(dissectKey.skipRightPadding(), is(true));

        dissectKey = new DissectKey("+" + keyName + "/2->");
        assertThat(dissectKey.skipRightPadding(), is(true));
    }

    public void testMultipleLeftModifiers() {
        String keyName = randomAlphaOfLengthBetween(1, 10);
        List<String> validModifiers = EnumSet.allOf(DissectKey.Modifier.class).stream()
            .filter(m -> m.equals(DissectKey.Modifier.NONE) == false)
            .map(DissectKey.Modifier::toString)
            .collect(Collectors.toList());
        String modifier1 = randomFrom(validModifiers);
        String modifier2 = randomFrom(validModifiers);
        DissectException e = expectThrows(DissectException.class, () -> new DissectKey(modifier1 + modifier2 + keyName));
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to parse key"));
    }

    public void testSkipKey() {
        String keyName = "";
        DissectKey dissectKey = new DissectKey(keyName);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.NONE));
        assertThat(dissectKey.skip(), is(true));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }
    public void testNamedSkipKey() {
        String keyName = "myname";
        DissectKey dissectKey = new DissectKey("?" +keyName);
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.NAMED_SKIP));
        assertThat(dissectKey.skip(), is(true));
        assertThat(dissectKey.skipRightPadding(), is(false));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testSkipKeyWithPadding() {
        String keyName = "";
        DissectKey dissectKey = new DissectKey(keyName  + "->");
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.NONE));
        assertThat(dissectKey.skip(), is(true));
        assertThat(dissectKey.skipRightPadding(), is(true));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }
    public void testNamedEmptySkipKeyWithPadding() {
        String keyName = "";
        DissectKey dissectKey = new DissectKey("?" +keyName + "->");
        assertThat(dissectKey.getModifier(), equalTo(DissectKey.Modifier.NAMED_SKIP));
        assertThat(dissectKey.skip(), is(true));
        assertThat(dissectKey.skipRightPadding(), is(true));
        assertThat(dissectKey.getAppendPosition(), equalTo(0));
        assertThat(dissectKey.getName(), equalTo(keyName));
    }

    public void testInvalidModifiers() {
        //should never happen due to regex
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DissectKey.Modifier.fromString("x"));
        assertThat(e.getMessage(), CoreMatchers.containsString("invalid modifier"));
    }
}
