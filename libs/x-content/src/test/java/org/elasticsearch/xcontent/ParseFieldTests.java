/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xcontent;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

public class ParseFieldTests extends ESTestCase {
    public void testParse() {
        String name = "foo_bar";
        ParseField field = new ParseField(name);
        String[] deprecated = new String[] { "barFoo", "bar_foo", "Foobar" };
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
        String[] deprecated = new String[] { "text", "same_as_text" };
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
        String[] alternatives = new String[] { "old_dep", "new_dep" };
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

    class TestDeprecationHandler implements DeprecationHandler {

        public boolean compatibleWarningsUsed = false;

        @Override
        public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {}

        @Override
        public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {}

        @Override
        public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {}

        @Override
        public void logRenamedField(
            String parserName,
            Supplier<XContentLocation> location,
            String oldName,
            String currentName,
            boolean isCompatibleDeprecation
        ) {
            this.compatibleWarningsUsed = isCompatibleDeprecation;
        }

        @Override
        public void logReplacedField(
            String parserName,
            Supplier<XContentLocation> location,
            String oldName,
            String replacedName,
            boolean isCompatibleDeprecation
        ) {
            this.compatibleWarningsUsed = isCompatibleDeprecation;

        }

        @Override
        public void logRemovedField(
            String parserName,
            Supplier<XContentLocation> location,
            String removedName,
            boolean isCompatibleDeprecation
        ) {
            this.compatibleWarningsUsed = isCompatibleDeprecation;
        }
    }

    public void testCompatibleLoggerIsUsed() {
        {
            // a field deprecated in previous version and now available under old name only in compatible api
            // emitting compatible logs
            ParseField field = new ParseField("new_name", "old_name").forRestApiVersion(
                RestApiVersion.equalTo(RestApiVersion.minimumSupported())
            );

            TestDeprecationHandler testDeprecationHandler = new TestDeprecationHandler();

            assertTrue(field.match("old_name", testDeprecationHandler));
            assertThat(testDeprecationHandler.compatibleWarningsUsed, is(true));
        }

        {
            // a regular newly deprecated field. Emitting deprecation logs instead of compatible logs
            ParseField field = new ParseField("new_name", "old_name");

            TestDeprecationHandler testDeprecationHandler = new TestDeprecationHandler();

            assertTrue(field.match("old_name", testDeprecationHandler));
            assertThat(testDeprecationHandler.compatibleWarningsUsed, is(false));
        }

    }

    public void testCompatibleWarnings() {
        ParseField field = new ParseField("new_name", "old_name").forRestApiVersion(
            RestApiVersion.equalTo(RestApiVersion.minimumSupported())
        );

        assertTrue(field.match("new_name", LoggingDeprecationHandler.INSTANCE));
        ensureNoWarnings();
        assertTrue(field.match("old_name", LoggingDeprecationHandler.INSTANCE));
        assertCriticalWarnings("Deprecated field [old_name] used, expected [new_name] instead");

        ParseField allDepField = new ParseField("dep", "old_name").withAllDeprecated()
            .forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.minimumSupported()));

        assertTrue(allDepField.match("dep", LoggingDeprecationHandler.INSTANCE));
        assertCriticalWarnings("Deprecated field [dep] used, this field is unused and will be removed entirely");
        assertTrue(allDepField.match("old_name", LoggingDeprecationHandler.INSTANCE));
        assertCriticalWarnings("Deprecated field [old_name] used, this field is unused and will be removed entirely");

        ParseField regularField = new ParseField("new_name");
        assertTrue(regularField.match("new_name", LoggingDeprecationHandler.INSTANCE));
        ensureNoWarnings();
    }
}
