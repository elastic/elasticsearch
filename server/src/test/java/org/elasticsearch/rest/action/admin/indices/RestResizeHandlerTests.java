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

import static org.mockito.Mockito.mock;

public class RestResizeHandlerTests extends ESTestCase {

    public void testShrinkCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestShrinkIndexAction handler =
                new RestResizeHandler.RestShrinkIndexAction(Settings.EMPTY, mock(RestController.class));
        final String copySettings = randomFrom("true", "false");
        final FakeRestRequest request =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                        .withParams(Collections.singletonMap("copy_settings", copySettings))
                        .withPath("source/_shrink/target")
                        .build();
        handler.prepareRequest(request, mock(NodeClient.class));
        assertWarnings("parameter [copy_settings] is deprecated but was [" + copySettings + "]");
    }

    public void testSplitCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestSplitIndexAction handler =
                new RestResizeHandler.RestSplitIndexAction(Settings.EMPTY, mock(RestController.class));
        final String copySettings = randomFrom("true", "false");
        final FakeRestRequest request =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                        .withParams(Collections.singletonMap("copy_settings", copySettings))
                        .withPath("source/_split/target")
                        .build();
        handler.prepareRequest(request, mock(NodeClient.class));
        assertWarnings("parameter [copy_settings] is deprecated but was [" + copySettings + "]");
    }

}
