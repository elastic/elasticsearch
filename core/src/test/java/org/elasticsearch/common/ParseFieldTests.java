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

import org.elasticsearch.common.ParseField.DeprecationHandler;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.collection.IsArrayContainingInAnyOrder.arrayContainingInAnyOrder;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ParseFieldTests extends ESTestCase {
    private final DeprecationHandler failOnDeprecation = new DeprecationHandler() {
        @Override
        public void usedDeprecatedName(String usedName, String modernName) {
            fail("expected no deprecation but used a deprecated name [" + usedName + "] for [" + modernName + "]");
        }

        @Override
        public void usedDeprecatedField(String usedName, String replacedWith) {
            fail("expected no deprecation but used a deprecated field [" + usedName + "] for [" + replacedWith + "]");
        }
    };

    public void testParse() {
        String name = "foo_bar";
        ParseField field = new ParseField(name);
        String[] deprecated = new String[]{"barFoo", "bar_foo", "Foobar"};
        ParseField withDeprecations = field.withDeprecation(deprecated);
        assertThat(field, not(sameInstance(withDeprecations)));
        assertThat(field.match(name, failOnDeprecation), is(true));
        assertThat(field.match("foo bar", failOnDeprecation), is(false));
        for (String deprecatedName : deprecated) {
            assertThat(field.match(deprecatedName, failOnDeprecation), is(false));
        }

        assertThat(withDeprecations.match(name, failOnDeprecation), is(true));
        assertThat(withDeprecations.match("foo bar", failOnDeprecation), is(false));
        for (String deprecatedName : deprecated) {
            ArgumentCaptor<String> calledWithDeprecatedName = ArgumentCaptor.forClass(String.class);
            DeprecationHandler handler = mock(DeprecationHandler.class);
            doNothing().when(handler).usedDeprecatedName(calledWithDeprecatedName.capture(), eq(name));
            assertTrue(withDeprecations.match(deprecatedName, handler));
            assertEquals(deprecatedName, calledWithDeprecatedName.getValue());
            verifyNoMoreInteractions(handler);
        }
    }

    public void testAllDeprecated() {
        String name = "like_text";
        String[] deprecated = new String[]{"text", "same_as_text"};
        ParseField field = new ParseField(name).withDeprecation(deprecated).withAllDeprecated("like");
        assertFalse(field.match("not a field name", failOnDeprecation));
        ArgumentCaptor<String> calledWithDeprecatedName = ArgumentCaptor.forClass(String.class);
        DeprecationHandler handler = mock(DeprecationHandler.class);
        assertTrue(field.match("text", handler));
        assertEquals(calledWithDeprecatedName.getValue(), "Deprecated field [text] used, replaced by [like]");
        assertTrue(field.match("same_as_text", handler));
        assertEquals(calledWithDeprecatedName.getValue(), "Deprecated field [same_as_text] used, replaced by [like]");
        assertTrue(field.match("like_text", handler));
        assertEquals(calledWithDeprecatedName.getValue(), "Deprecated field [like_text] used, replaced by [like]");
        verifyNoMoreInteractions(handler);
    }

    public void testGetAllNamesIncludedDeprecated() {
        ParseField parseField = new ParseField("terms", "in");
        assertThat(parseField.getAllNamesIncludedDeprecated(), arrayContainingInAnyOrder("terms", "in"));

        parseField = new ParseField("more_like_this", "mlt");
        assertThat(parseField.getAllNamesIncludedDeprecated(), arrayContainingInAnyOrder("more_like_this", "mlt"));
    }
}
