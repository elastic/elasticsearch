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

package org.elasticsearch.ingest;

import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.node.NodeModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

public class IngestCloseIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(IngestPlugin.class);
    }

    private static AtomicBoolean called = new AtomicBoolean(false);

    public void testCloseNode() throws Exception {
        // We manually stop the node and check we called
        stopNode();

        assertThat(called.get(), is(true));

        // We need to restart the node for the next tests (and because tearDown() expects a Node)
        startNode();
    }

    public static class IngestPlugin extends Plugin {
        public void onModule(NodeModule nodeModule) {
            nodeModule.registerProcessor("test", (registry) -> new Factory());
        }
    }

    public static final class Factory extends AbstractProcessorFactory<TestProcessor> implements Closeable {
        @Override
        protected TestProcessor doCreate(String tag, Map<String, Object> config) throws Exception {
            return new TestProcessor("id", "test", ingestDocument -> {
                throw new UnsupportedOperationException("this code is actually never called from the test");
            });
        }

        @Override
        public void close() throws IOException {
            called.set(true);
        }
    }

}
