/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;

public class RestResizeHandlerTests extends ESTestCase {

    public void testShrinkCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestShrinkIndexAction handler = new RestResizeHandler.RestShrinkIndexAction();
        for (final String copySettings : new String[] { null, "", "true", "false" }) {
            runTestResizeCopySettingsDeprecated(handler, "shrink", copySettings);
        }
    }

    public void testSplitCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestSplitIndexAction handler = new RestResizeHandler.RestSplitIndexAction();
        for (final String copySettings : new String[] { null, "", "true", "false" }) {
            runTestResizeCopySettingsDeprecated(handler, "split", copySettings);
        }
    }

    private void runTestResizeCopySettingsDeprecated(
        final RestResizeHandler handler,
        final String resizeOperation,
        final String copySettings
    ) throws IOException {
        final FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Collections.singletonMap("copy_settings", copySettings)
        ).withPath(String.format(Locale.ROOT, "source/_%s/target", resizeOperation));
        if (copySettings != null) {
            builder.withParams(Collections.singletonMap("copy_settings", copySettings));
        }
        final FakeRestRequest request = builder.build();
        if ("false".equals(copySettings)) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> handler.prepareRequest(request, mock(NodeClient.class))
            );
            assertThat(e, hasToString(containsString("parameter [copy_settings] can not be explicitly set to [false]")));
        } else {
            handler.prepareRequest(request, mock(NodeClient.class));
            if ("".equals(copySettings) || "true".equals(copySettings)) {
                assertWarnings("parameter [copy_settings] is deprecated and will be removed in 8.0.0");
            }
        }
    }

}
