/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.nullValue;

public class RestReloadSecureSettingsActionTests extends ESTestCase {

    public void testParserWithPassword() throws Exception {
        final String request = "{" +
            "\"secure_settings_password\": \"secure_settings_password_string\"" +
            "}";
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, request)) {
            NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = RestReloadSecureSettingsAction.PARSER.parse(parser, null);
            assertEquals("secure_settings_password_string", reloadSecureSettingsRequest.getSecureSettingsPassword().toString());
        }
    }

    public void testParserWithoutPassword() throws Exception {
        final String request = "{" +
            "}";
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, request)) {
            NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = RestReloadSecureSettingsAction.PARSER.parse(parser, null);
            assertThat(reloadSecureSettingsRequest.getSecureSettingsPassword(), nullValue());
        }
    }
}
