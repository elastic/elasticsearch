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
            .filter(m -> !m.equals(DissectKey.Modifier.NONE))
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
