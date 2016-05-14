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

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.collection.IsArrayContainingInAnyOrder.arrayContainingInAnyOrder;

public class ParseFieldTests extends ESTestCase {
    public void testParse() {
        String name = "foo_bar";
        ParseField field = new ParseField(name);
        String[] deprecated = new String[]{"barFoo", "bar_foo", "Foobar"};
        ParseField withDeprecations = field.withDeprecation(deprecated);
        assertThat(field, not(sameInstance(withDeprecations)));
        assertThat(field.match(name, false), is(true));
        assertThat(field.match("foo bar", false), is(false));
        for (String deprecatedName : deprecated) {
            assertThat(field.match(deprecatedName, false), is(false));
        }

        assertThat(withDeprecations.match(name, false), is(true));
        assertThat(withDeprecations.match("foo bar", false), is(false));
        for (String deprecatedName : deprecated) {
            assertThat(withDeprecations.match(deprecatedName, false), is(true));
        }

        // now with strict mode
        assertThat(field.match(name, true), is(true));
        assertThat(field.match("foo bar", true), is(false));
        for (String deprecatedName : deprecated) {
            assertThat(field.match(deprecatedName, true), is(false));
        }

        assertThat(withDeprecations.match(name, true), is(true));
        assertThat(withDeprecations.match("foo bar", true), is(false));
        for (String deprecatedName : deprecated) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
                withDeprecations.match(deprecatedName, true);
            });
            assertThat(e.getMessage(), containsString("used, expected [foo_bar] instead"));
        }
    }

    public void testAllDeprecated() {
        String name = "like_text";

        boolean withDeprecatedNames = randomBoolean();
        String[] deprecated = new String[]{"text", "same_as_text"};
        String[] allValues;
        if (withDeprecatedNames) {
            String[] newArray = new String[1 + deprecated.length];
            newArray[0] = name;
            System.arraycopy(deprecated, 0, newArray, 1, deprecated.length);
            allValues = newArray;
        } else {
            allValues = new String[] {name};
        }

        ParseField field;
        if (withDeprecatedNames) {
            field = new ParseField(name).withDeprecation(deprecated).withAllDeprecated("like");
        } else {
            field = new ParseField(name).withAllDeprecated("like");
        }

        // strict mode off
        assertThat(field.match(randomFrom(allValues), false), is(true));
        assertThat(field.match("not a field name", false), is(false));

        // now with strict mode
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> field.match(randomFrom(allValues), true));
        assertThat(e.getMessage(), containsString(" used, replaced by [like]"));
    }

    public void testGetAllNamesIncludedDeprecated() {
        ParseField parseField = new ParseField("terms", "in");
        assertThat(parseField.getAllNamesIncludedDeprecated(), arrayContainingInAnyOrder("terms", "in"));

        parseField = new ParseField("more_like_this", "mlt");
        assertThat(parseField.getAllNamesIncludedDeprecated(), arrayContainingInAnyOrder("more_like_this", "mlt"));
    }
}
