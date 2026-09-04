/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.template;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class TemplateUtilsTests extends ESTestCase {

    private static final String SIMPLE_TEST_TEMPLATE = "/monitoring-%s.json";
    private static final String TEST_TEMPLATE_WITH_VARIABLES = "/template_with_variables-test.json";

    public void testLoadTemplate() throws IOException {
        final int version = randomIntBetween(0, 10_000);
        String resource = Strings.format(SIMPLE_TEST_TEMPLATE, "test");
        String source = TemplateUtils.loadTemplate(resource, String.valueOf(version), "monitoring.template.version", emptyMap());

        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
        assertTemplate(XContentHelper.stripWhitespace(source), equalTo(XContentHelper.stripWhitespace(Strings.format("""
            {
              "index_patterns": ".monitoring-data-%s",
              "mappings": {
                "doc": {
                  "_meta": {
                    "template.version": "%s"
                  }
                }
              }
            }""", version, version))));
    }

    public void testLoadTemplate_GivenTemplateWithVariables() throws IOException {
        final int version = randomIntBetween(0, 10_000);
        Map<String, String> variables = new HashMap<>();
        variables.put("test.template.field_1", "test_field_1");
        variables.put("test.template.field_2", """
            "test_field_2": {"type": "long"}""");

        String source = TemplateUtils.loadTemplate(
            TEST_TEMPLATE_WITH_VARIABLES,
            String.valueOf(version),
            "test.template.version",
            variables
        );

        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
        assertTemplate(XContentHelper.stripWhitespace(source), equalTo(XContentHelper.stripWhitespace(Strings.format("""
            {
              "index_patterns": ".test-%s",
              "mappings": {
                "doc": {
                  "_meta": {
                    "template.version": "%s"
                  },
                  "properties": {
                    "test_field_1": {
                      "type": "keyword"
                    },
                    "test_field_2": {
                      "type": "long"
                    }
                  }
                }
              }
            }""", version, version))));
    }

    public void testLoadTemplateWithTemplateDecorator() throws IOException {
        String resource = "/settings-only.json";
        Template.TemplateDecorator keepRefreshIntervalOnly = new Template.TemplateDecorator() {
            @Override
            public Settings decorate(String template, Settings settings) {
                return settings.filter("index.refresh_interval"::equals);
            }
        };

        var template = TemplateUtils.loadTemplate(resource, "1", "version", emptyMap(), false, Template::parse, keepRefreshIntervalOnly);
        assertThat(template.settings().keySet(), equalTo(Set.of("index.refresh_interval")));
    }

    public void testValidateNullSource() {
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> TemplateUtils.validate(null, "version", false)
        );
        assertThat(exception.getMessage(), is("Template must not be null"));
    }

    public void testValidateEmptySource() {
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> TemplateUtils.validate("", "version", false)
        );
        assertThat(exception.getMessage(), is("Template must not be empty"));
    }

    public void testValidateInvalidSource() {
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> TemplateUtils.validate("{\"foo\": \"bar", "version", false)
        );
        assertThat(exception.getMessage(), is("Invalid template"));
    }

    public void testValidate() throws IOException {
        String resource = Strings.format(SIMPLE_TEST_TEMPLATE, "test");
        try (InputStream is = TemplateUtilsTests.class.getResourceAsStream(resource)) {
            assert is != null;
            TemplateUtils.validate(new String(is.readAllBytes(), StandardCharsets.UTF_8), "version", false);
        }
    }

    public void testValidateWithValidateVersion() {
        String withVersion = "{\"index_patterns\": [\"test-*\"], \"version\": 42}";
        TemplateUtils.validate(withVersion, "42", true);

        var exception = expectThrows(IllegalArgumentException.class, () -> TemplateUtils.validate(withVersion, "99", true));
        assertThat(exception.getMessage(), is("Template must have a version property set to the given version property"));

        String noVersion = "{\"index_patterns\": [\"test-*\"]}";
        exception = expectThrows(IllegalArgumentException.class, () -> TemplateUtils.validate(noVersion, "99", true));
        assertThat(exception.getMessage(), is("Template must have a version property set to the given version property"));
    }

    public void testReplaceVariable() {
        assertTemplate(TemplateUtils.replaceVariable("${monitoring.template.version}", "monitoring.template.version", "0"), equalTo("0"));
        assertTemplate(
            TemplateUtils.replaceVariable("{\"template\": \"test-${monitoring.template.version}\"}", "monitoring.template.version", "1"),
            equalTo("{\"template\": \"test-1\"}")
        );
        assertTemplate(
            TemplateUtils.replaceVariable("{\"template\": \"${monitoring.template.version}-test\"}", "monitoring.template.version", "2"),
            equalTo("{\"template\": \"2-test\"}")
        );
        assertTemplate(
            TemplateUtils.replaceVariable("""
                {"template": "test-${monitoring.template.version}-test"}""", "monitoring.template.version", "3"),
            equalTo("{\"template\": \"test-3-test\"}")
        );

        final int version = randomIntBetween(0, 100);
        assertTemplate(TemplateUtils.replaceVariable("""
            {"foo-${monitoring.template.version}": "bar-${monitoring.template.version}"}
            """, "monitoring.template.version", String.valueOf(version)), equalTo(Strings.format("""
            {"foo-%s": "bar-%s"}
            """, version, version)));
    }

    public static void assertTemplate(String actual, Matcher<? super String> matcher) {
        if (Constants.WINDOWS) {
            // translate Windows line endings (\r\n) to standard ones (\n)
            actual = Strings.replace(actual, System.lineSeparator(), "\n");
        }
        assertThat(actual, matcher);
    }

}
