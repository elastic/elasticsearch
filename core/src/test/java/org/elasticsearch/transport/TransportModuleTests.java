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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.threadpool.ThreadPool;

/** Unit tests for module registering custom transport and transport service */
public class TransportModuleTests extends ModuleTestCase {



    static class FakeTransport extends AssertingLocalTransport {
        @Inject
        public FakeTransport(Settings settings, CircuitBreakerService circuitBreakerService, ThreadPool threadPool, Version version,
                             NamedWriteableRegistry namedWriteableRegistry) {
            super(settings, circuitBreakerService, threadPool, version, namedWriteableRegistry);
        }
    }

    static class FakeTransportService extends TransportService {
        @Inject
        public FakeTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
            super(settings, transport, threadPool);
        }
    }
}
