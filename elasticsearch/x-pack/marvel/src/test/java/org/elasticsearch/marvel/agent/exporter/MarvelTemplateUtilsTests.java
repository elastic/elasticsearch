/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class MarvelTemplateUtilsTests extends ESTestCase {

    private static final String TEST_TEMPLATE = "/monitoring-test.json";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public void testLoadTemplate() throws IOException {
        final int version = randomIntBetween(0, 10_000);
        String source = MarvelTemplateUtils.loadTemplate("test", version);

        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
        assertTemplate(source, equalTo("{\n" +
                "  \"template\": \".monitoring-data-" + version + "\",\n" +
                "  \"mappings\": {\n" +
                "    \"type_1\": {\n" +
                "      \"_meta\": {\n" +
                "        \"template.version\": \"" + version + "\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}"));
    }

    public void testLoad() throws IOException {
        BytesReference source = MarvelTemplateUtils.load(TEST_TEMPLATE);
        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
    }

    public void testValidateNullSource() {
        expectedException.expect(ElasticsearchParseException.class);
        expectedException.expectMessage("Monitoring template must not be null");
        MarvelTemplateUtils.validate(null);
    }

    public void testValidateEmptySource() {
        expectedException.expect(ElasticsearchParseException.class);
        expectedException.expectMessage("Monitoring template must not be empty");
        MarvelTemplateUtils.validate(new BytesArray(""));
    }

    public void testValidateInvalidSource() {
        expectedException.expect(ElasticsearchParseException.class);
        expectedException.expectMessage("Invalid monitoring template");
        MarvelTemplateUtils.validate(new BytesArray("{\"foo\": \"bar"));
    }

    public void testValidate() {
        try {
            MarvelTemplateUtils.validate(MarvelTemplateUtils.load(TEST_TEMPLATE));
        } catch (Exception e) {
            fail("failed to validate test template: " + e.getMessage());
        }
    }

    public void testFilter() {
        assertTemplate(MarvelTemplateUtils.filter(new BytesArray("${monitoring.template.version}"), 0), equalTo("0"));
        assertTemplate(MarvelTemplateUtils.filter(new BytesArray("{\"template\": \"test-${monitoring.template.version}\"}"), 1),
                equalTo("{\"template\": \"test-1\"}"));
        assertTemplate(MarvelTemplateUtils.filter(new BytesArray("{\"template\": \"${monitoring.template.version}-test\"}"), 2),
                equalTo("{\"template\": \"2-test\"}"));
        assertTemplate(MarvelTemplateUtils.filter(new BytesArray("{\"template\": \"test-${monitoring.template.version}-test\"}"), 3),
                equalTo("{\"template\": \"test-3-test\"}"));

        final int version = randomIntBetween(0, 100);
        assertTemplate(MarvelTemplateUtils.filter(new BytesArray("{\"foo-${monitoring.template.version}\": " +
                        "\"bar-${monitoring.template.version}\"}"), version),
                equalTo("{\"foo-" + version + "\": \"bar-" + version + "\"}"));
    }

    public static void assertTemplate(String actual, Matcher<? super String> matcher) {
        if (Constants.WINDOWS) {
            // translate Windows line endings (\r\n) to standard ones (\n)
            actual = Strings.replace(actual, System.lineSeparator(), "\n");
        }
        assertThat(actual, matcher);
    }

}
