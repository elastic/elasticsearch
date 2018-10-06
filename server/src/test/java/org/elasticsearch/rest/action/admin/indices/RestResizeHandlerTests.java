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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;

public class RestResizeHandlerTests extends ESTestCase {

    public void testShrinkCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestShrinkIndexAction handler =
                new RestResizeHandler.RestShrinkIndexAction(Settings.EMPTY, mock(RestController.class));
        for (final String copySettings : new String[]{null, "", "true", "false"}) {
            runTestResizeCopySettingsDeprecated(handler, "shrink", copySettings);
        }
    }

    public void testSplitCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestSplitIndexAction handler =
                new RestResizeHandler.RestSplitIndexAction(Settings.EMPTY, mock(RestController.class));
        for (final String copySettings : new String[]{null, "", "true", "false"}) {
            runTestResizeCopySettingsDeprecated(handler, "split", copySettings);
        }
    }

    private void runTestResizeCopySettingsDeprecated(
            final RestResizeHandler handler, final String resizeOperation, final String copySettings) throws IOException {
        final FakeRestRequest.Builder builder =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                        .withParams(Collections.singletonMap("copy_settings", copySettings))
                        .withPath(String.format(Locale.ROOT, "source/_%s/target", resizeOperation));
        if (copySettings != null) {
            builder.withParams(Collections.singletonMap("copy_settings", copySettings));
        }
        final FakeRestRequest request = builder.build();
        if ("false".equals(copySettings)) {
            final IllegalArgumentException e =
                    expectThrows(IllegalArgumentException.class, () -> handler.prepareRequest(request, mock(NodeClient.class)));
            assertThat(e, hasToString(containsString("parameter [copy_settings] can not be explicitly set to [false]")));
        } else {
            handler.prepareRequest(request, mock(NodeClient.class));
            if ("".equals(copySettings) || "true".equals(copySettings)) {
                assertWarnings("parameter [copy_settings] is deprecated and will be removed in 8.0.0");
            }
        }
    }

}
