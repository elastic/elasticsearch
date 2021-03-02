/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

public class ParseFieldTests extends ESTestCase {
    public void testParse() {
        String name = "foo_bar";
        ParseField field = new ParseField(name);
        String[] deprecated = new String[]{"barFoo", "bar_foo", "Foobar"};
        ParseField withDeprecations = field.withDeprecation(deprecated);
        assertThat(field, not(sameInstance(withDeprecations)));
        assertThat(field.match(name, LoggingDeprecationHandler.INSTANCE), is(true));
        assertThat(field.match("foo bar", LoggingDeprecationHandler.INSTANCE), is(false));
        for (String deprecatedName : deprecated) {
            assertThat(field.match(deprecatedName, LoggingDeprecationHandler.INSTANCE), is(false));
        }

        assertThat(withDeprecations.match(name, LoggingDeprecationHandler.INSTANCE), is(true));
        assertThat(withDeprecations.match("foo bar", LoggingDeprecationHandler.INSTANCE), is(false));
        for (String deprecatedName : deprecated) {
            assertThat(withDeprecations.match(deprecatedName, LoggingDeprecationHandler.INSTANCE), is(true));
            assertWarnings("Deprecated field [" + deprecatedName + "] used, expected [foo_bar] instead");
        }
    }

    public void testAllDeprecated() {
        String name = "like_text";
        String[] deprecated = new String[]{"text", "same_as_text"};
        ParseField field = new ParseField(name).withDeprecation(deprecated).withAllDeprecated("like");
        assertFalse(field.match("not a field name", LoggingDeprecationHandler.INSTANCE));
        assertTrue(field.match("text", LoggingDeprecationHandler.INSTANCE));
        assertWarnings("Deprecated field [text] used, replaced by [like]");
        assertTrue(field.match("same_as_text", LoggingDeprecationHandler.INSTANCE));
        assertWarnings("Deprecated field [same_as_text] used, replaced by [like]");
        assertTrue(field.match("like_text", LoggingDeprecationHandler.INSTANCE));
        assertWarnings("Deprecated field [like_text] used, replaced by [like]");
    }

    public void testDeprecatedWithNoReplacement() {
        String name = "dep";
        String[] alternatives = new String[]{"old_dep", "new_dep"};
        ParseField field = new ParseField(name).withDeprecation(alternatives).withAllDeprecated();
        assertFalse(field.match("not a field name", LoggingDeprecationHandler.INSTANCE));
        assertTrue(field.match("dep", LoggingDeprecationHandler.INSTANCE));
        assertWarnings("Deprecated field [dep] used, this field is unused and will be removed entirely");
        assertTrue(field.match("old_dep", LoggingDeprecationHandler.INSTANCE));
        assertWarnings("Deprecated field [old_dep] used, this field is unused and will be removed entirely");
        assertTrue(field.match("new_dep", LoggingDeprecationHandler.INSTANCE));
        assertWarnings("Deprecated field [new_dep] used, this field is unused and will be removed entirely");


    }

    public void testGetAllNamesIncludedDeprecated() {
        ParseField parseField = new ParseField("terms", "in");
        assertThat(parseField.getAllNamesIncludedDeprecated(), arrayContainingInAnyOrder("terms", "in"));

        parseField = new ParseField("more_like_this", "mlt");
        assertThat(parseField.getAllNamesIncludedDeprecated(), arrayContainingInAnyOrder("more_like_this", "mlt"));
    }
}
