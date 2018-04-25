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

package org.elasticsearch.client.transport;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.MockTransportClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;

public class TransportClientTests extends ESTestCase {

    public void testThatUsingAClosedClientThrowsAnException() throws ExecutionException, InterruptedException {
        final TransportClient client =  new MockTransportClient(Settings.EMPTY);
        client.close();
        final IllegalStateException e =
            expectThrows(IllegalStateException.class, () -> client.admin().cluster().health(new ClusterHealthRequest()).get());
        assertThat(e, hasToString(containsString("transport client is closed")));
    }

    /**
     * test that when plugins are provided that want to register
     * {@link NamedWriteable}, those are also made known to the
     * {@link NamedWriteableRegistry} of the transport client
     */
    public void testPluginNamedWriteablesRegistered() {
        Settings baseSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();
        try (TransportClient client = new MockTransportClient(baseSettings, Arrays.asList(MockPlugin.class))) {
            assertNotNull(client.namedWriteableRegistry.getReader(MockPlugin.MockNamedWriteable.class, MockPlugin.MockNamedWriteable.NAME));
        }
    }

    public static class MockPlugin extends Plugin {

        @Override
        public List<Entry> getNamedWriteables() {
            return Arrays.asList(new Entry[]{ new Entry(MockNamedWriteable.class, MockNamedWriteable.NAME, MockNamedWriteable::new)});
        }

        public class MockNamedWriteable implements NamedWriteable {

            static final String NAME = "mockNamedWritable";

            MockNamedWriteable(StreamInput in) {
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }

            @Override
            public String getWriteableName() {
                return NAME;
            }

        }
    }
}
