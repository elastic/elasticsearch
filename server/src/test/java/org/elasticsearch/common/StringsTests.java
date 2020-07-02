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

package org.elasticsearch.common;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class StringsTests extends ESTestCase {

    public void testIsAllOrWildCardString() {
        assertThat(Strings.isAllOrWildcard("_all"), is(true));
        assertThat(Strings.isAllOrWildcard("*"), is(true));
        assertThat(Strings.isAllOrWildcard("foo"), is(false));
        assertThat(Strings.isAllOrWildcard(""), is(false));
        assertThat(Strings.isAllOrWildcard((String)null), is(false));
    }

    public void testSubstring() {
        assertEquals(null, Strings.substring(null, 0, 1000));
        assertEquals("foo", Strings.substring("foo", 0, 1000));
        assertEquals("foo", Strings.substring("foo", 0, 3));
        assertEquals("oo", Strings.substring("foo", 1, 3));
        assertEquals("oo", Strings.substring("foo", 1, 100));
        assertEquals("f", Strings.substring("foo", 0, 1));
    }

    public void testCleanTruncate() {
        assertEquals(null, Strings.cleanTruncate(null, 10));
        assertEquals("foo", Strings.cleanTruncate("foo", 10));
        assertEquals("foo", Strings.cleanTruncate("foo", 3));
        // Throws out high surrogates
        assertEquals("foo", Strings.cleanTruncate("foo\uD83D\uDEAB", 4));
        // But will keep the whole character
        assertEquals("foo\uD83D\uDEAB", Strings.cleanTruncate("foo\uD83D\uDEAB", 5));
        /*
         * Doesn't take care around combining marks. This example has its
         * meaning changed because that last codepoint is supposed to combine
         * backwards into the find "o" and be represented as the "o" with a
         * circle around it with a slash through it. As in "no 'o's allowed
         * here.
         */
        assertEquals("o", Strings.cleanTruncate("o\uD83D\uDEAB", 1));
        assertEquals("", Strings.cleanTruncate("foo", 0));
    }

    public void testToStringToXContent() {
        final ToXContent toXContent;
        final boolean error;
        if (randomBoolean()) {
            if (randomBoolean()) {
                error = false;
                toXContent  = (builder, params) -> builder.field("ok", "here").field("catastrophe", "");
            } else {
                error = true;
                toXContent  = (builder, params) ->
                        builder.startObject().field("ok", "here").field("catastrophe", "").endObject();
            }
        } else {
            if (randomBoolean()) {
                error = false;
                toXContent = (ToXContentObject) (builder, params) ->
                        builder.startObject().field("ok", "here").field("catastrophe", "").endObject();
            } else {
                error = true;
                toXContent = (ToXContentObject) (builder, params) -> builder.field("ok", "here").field("catastrophe", "");
            }
        }

        String toString = Strings.toString(toXContent);
        if (error) {
            assertThat(toString, containsString("\"error\":\"error building toString out of XContent:"));
            assertThat(toString, containsString("\"stack_trace\":"));
        } else {
            assertThat(toString, containsString("\"ok\":\"here\""));
            assertThat(toString, containsString("\"catastrophe\":\"\""));
        }
    }

    public void testSplitStringToSet() {
        assertEquals(Strings.tokenizeByCommaToSet(null), Sets.newHashSet());
        assertEquals(Strings.tokenizeByCommaToSet(""), Sets.newHashSet());
        assertEquals(Strings.tokenizeByCommaToSet("a,b,c"), Sets.newHashSet("a","b","c"));
        assertEquals(Strings.tokenizeByCommaToSet("a, b, c"), Sets.newHashSet("a","b","c"));
        assertEquals(Strings.tokenizeByCommaToSet(" a ,  b, c  "), Sets.newHashSet("a","b","c"));
        assertEquals(Strings.tokenizeByCommaToSet("aa, bb, cc"), Sets.newHashSet("aa","bb","cc"));
        assertEquals(Strings.tokenizeByCommaToSet(" a "), Sets.newHashSet("a"));
        assertEquals(Strings.tokenizeByCommaToSet("   a   "), Sets.newHashSet("a"));
        assertEquals(Strings.tokenizeByCommaToSet("   aa   "), Sets.newHashSet("aa"));
        assertEquals(Strings.tokenizeByCommaToSet("   "), Sets.newHashSet());
    }
}
