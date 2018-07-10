/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.template;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class TemplateUtilsTests extends ESTestCase {

    private static final String TEST_TEMPLATE = "/monitoring-%s.json";

    public void testLoadTemplate() throws IOException {
        final int version = randomIntBetween(0, 10_000);
        String resource = String.format(Locale.ROOT, TEST_TEMPLATE, "test");
        String source = TemplateUtils.loadTemplate(resource, String.valueOf(version), Pattern.quote("${monitoring.template.version}"));

        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
        assertTemplate(source, equalTo("{\n" +
                "  \"index_patterns\": \".monitoring-data-" + version + "\",\n" +
                "  \"mappings\": {\n" +
                "    \"doc\": {\n" +
                "      \"_meta\": {\n" +
                "        \"template.version\": \"" + version + "\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n"));
    }

    public void testLoad() throws IOException {
        String resource = String.format(Locale.ROOT, TEST_TEMPLATE, "test");
        BytesReference source = TemplateUtils.load(resource);
        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
    }

    public void testValidateNullSource() {
        ElasticsearchParseException exception = expectThrows(ElasticsearchParseException.class, () -> TemplateUtils.validate(null));
        assertThat(exception.getMessage(), is("Template must not be null"));
    }

    public void testValidateEmptySource() {
        ElasticsearchParseException exception = expectThrows(ElasticsearchParseException.class,
                () -> TemplateUtils.validate(new BytesArray("")));
        assertThat(exception.getMessage(), is("Template must not be empty"));
    }

    public void testValidateInvalidSource() {
        ElasticsearchParseException exception = expectThrows(ElasticsearchParseException.class,
                () -> TemplateUtils.validate(new BytesArray("{\"foo\": \"bar")));
        assertThat(exception.getMessage(), is("Invalid template"));
    }

    public void testValidate() throws IOException {
        String resource = String.format(Locale.ROOT, TEST_TEMPLATE, "test");
        TemplateUtils.validate(TemplateUtils.load(resource));
    }

    public void testFilter() {
        assertTemplate(TemplateUtils.filter(new BytesArray("${monitoring.template.version}"), "0",
                Pattern.quote("${monitoring.template.version}")), equalTo("0"));
        assertTemplate(TemplateUtils.filter(new BytesArray("{\"template\": \"test-${monitoring.template.version}\"}"), "1",
                Pattern.quote("${monitoring.template.version}")), equalTo("{\"template\": \"test-1\"}"));
        assertTemplate(TemplateUtils.filter(new BytesArray("{\"template\": \"${monitoring.template.version}-test\"}"), "2",
                Pattern.quote("${monitoring.template.version}")), equalTo("{\"template\": \"2-test\"}"));
        assertTemplate(TemplateUtils.filter(new BytesArray("{\"template\": \"test-${monitoring.template.version}-test\"}"), "3",
                Pattern.quote("${monitoring.template.version}")), equalTo("{\"template\": \"test-3-test\"}"));

        final int version = randomIntBetween(0, 100);
        assertTemplate(TemplateUtils.filter(new BytesArray("{\"foo-${monitoring.template.version}\": " +
                        "\"bar-${monitoring.template.version}\"}"), String.valueOf(version),
                Pattern.quote("${monitoring.template.version}")),
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
