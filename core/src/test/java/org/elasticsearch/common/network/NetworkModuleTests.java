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

package org.elasticsearch.common.network;

import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerAdapter;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

public class NetworkModuleTests extends ModuleTestCase {

    static class FakeTransportService extends TransportService {
        public FakeTransportService() {
            super(null, null);
        }
    }

    static class FakeTransport extends AssertingLocalTransport {
        public FakeTransport() {
            super(null, null, null, null);
        }
    }

    static class FakeHttpTransport extends AbstractLifecycleComponent<HttpServerTransport> implements HttpServerTransport {
        public FakeHttpTransport() {
            super(null);
        }
        @Override
        protected void doStart() {}
        @Override
        protected void doStop() {}
        @Override
        protected void doClose() {}
        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }
        @Override
        public HttpInfo info() {
            return null;
        }
        @Override
        public HttpStats stats() {
            return null;
        }
        @Override
        public void httpServerAdapter(HttpServerAdapter httpServerAdapter) {}
    }

    public void testRegisterTransportService() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_SERVICE_TYPE_KEY, "custom").build();
        NetworkModule module = new NetworkModule(new NetworkService(settings), settings, false, Version.CURRENT);
        module.registerTransportService("custom", FakeTransportService.class);
        assertBinding(module, TransportService.class, FakeTransportService.class);

        // check it works with transport only as well
        module = new NetworkModule(new NetworkService(settings), settings, true, Version.CURRENT);
        module.registerTransportService("custom", FakeTransportService.class);
        assertBinding(module, TransportService.class, FakeTransportService.class);
    }

    public void testRegisterTransport() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "custom").build();
        NetworkModule module = new NetworkModule(new NetworkService(settings), settings, false, Version.CURRENT);
        module.registerTransport("custom", FakeTransport.class);
        assertBinding(module, Transport.class, FakeTransport.class);

        // check it works with transport only as well
        module = new NetworkModule(new NetworkService(settings), settings, true, Version.CURRENT);
        module.registerTransport("custom", FakeTransport.class);
        assertBinding(module, Transport.class, FakeTransport.class);
    }

    public void testRegisterHttpTransport() {
        Settings settings = Settings.builder().put(NetworkModule.HTTP_TYPE_KEY, "custom").build();
        NetworkModule module = new NetworkModule(new NetworkService(settings), settings, false, Version.CURRENT);
        module.registerHttpTransport("custom", FakeHttpTransport.class);
        assertBinding(module, HttpServerTransport.class, FakeHttpTransport.class);

        // check registration not allowed for transport only
        module = new NetworkModule(new NetworkService(settings), settings, true, Version.CURRENT);
        try {
            module.registerHttpTransport("custom", FakeHttpTransport.class);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Cannot register http transport"));
            assertTrue(e.getMessage().contains("for transport client"));
        }

        // not added if http is disabled
        settings = Settings.builder().put(NetworkModule.HTTP_ENABLED, false).build();
        module = new NetworkModule(new NetworkService(settings), settings, false, Version.CURRENT);
        assertNotBound(module, HttpServerTransport.class);
    }
}
