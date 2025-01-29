/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import static org.hamcrest.Matchers.nullValue;

public class RestReloadSecureSettingsActionTests extends ESTestCase {

    public void testParserWithPassword() throws Exception {
        final String request = """
            {"secure_settings_password": "secure_settings_password_string"}""";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, request)) {
            RestReloadSecureSettingsAction.ParsedRequestBody parsedRequestBody = RestReloadSecureSettingsAction.PARSER.parse(parser, null);
            assertEquals("secure_settings_password_string", parsedRequestBody.secureSettingsPassword.toString());
        }
    }

    public void testParserWithoutPassword() throws Exception {
        final String request = "{}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, request)) {
            RestReloadSecureSettingsAction.ParsedRequestBody parsedRequestBody = RestReloadSecureSettingsAction.PARSER.parse(parser, null);
            assertThat(parsedRequestBody.secureSettingsPassword, nullValue());
        }
    }
}
