/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 *  _cluster/settings request with deprecated setting is emitting the deprecation warning
 *  and has to be in a separate testcase because it is affecting other tests
 */
public class DeprecationSettingsIT extends ESRestTestCase {
    /**
     * Same as <code>DeprecationIndexingAppender#DEPRECATION_MESSAGES_DATA_STREAM</code>, but that class isn't visible from here.
     */
    private static final String DATA_STREAM_NAME = ".logs-deprecation.elasticsearch-default";

    /**
     * Builds a REST client that will tolerate warnings in the response headers. The default
     * is to throw an exception.
     */
    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    /**
     * Check that configuring deprecation settings causes a warning to be added to the
     * response headers.
     */
    public void testDeprecatedSettingsReturnWarnings() throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("persistent")
            .field(
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getKey(),
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getDefault(Settings.EMPTY) == false
            )
            .field(
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getKey(),
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getDefault(Settings.EMPTY) == false
            )
            // There should be no warning for this field
            .field(
                TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getKey(),
                TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getDefault(Settings.EMPTY) == false
            )
            .endObject()
            .endObject();

        final Request request = new Request("PUT", "_cluster/settings");
        ///
        request.setJsonEntity(Strings.toString(builder));
        final Response response = client().performRequest(request);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<String>> headerMatchers = new ArrayList<>(2);

        for (Setting<Boolean> setting : List.of(
            TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1,
            TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2
        )) {
            headerMatchers.add(
                equalTo(
                    "["
                        + setting.getKey()
                        + "] setting was deprecated in Elasticsearch and will be removed in a future release! "
                        + "See the breaking changes documentation for the next major version."
                )
            );
        }

        assertThat(deprecatedWarnings, hasSize(headerMatchers.size()));
        for (final String deprecatedWarning : deprecatedWarnings) {
            assertThat(
                "Header does not conform to expected pattern",
                deprecatedWarning,
                matches(HeaderWarning.WARNING_HEADER_PATTERN.pattern())
            );
        }

        final List<String> actualWarningValues = deprecatedWarnings.stream()
            .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
            .collect(Collectors.toList());
        for (Matcher<String> headerMatcher : headerMatchers) {
            assertThat(actualWarningValues, hasItem(headerMatcher));
        }
    }

    private List<String> getWarningHeaders(Header[] headers) {
        List<String> warnings = new ArrayList<>();

        for (Header header : headers) {
            if (header.getName().equals("Warning")) {
                warnings.add(header.getValue());
            }
        }

        return warnings;
    }

}
