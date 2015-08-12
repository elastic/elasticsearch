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
package org.elasticsearch.discovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.node.service.NodeService;

/**
 */
public class DiscoveryModuleTests extends ModuleTestCase {

    public static class DummyMasterElectionService extends ElectMasterService {

        public DummyMasterElectionService(Settings settings, Version version) {
            super(settings, version);
        }
    }


    public void testRegisterMasterElectionService() {
        Settings settings = Settings.builder().put("node.local", false).
                put(DiscoveryModule.ZEN_MASTER_SERVICE_TYPE_KEY, "custom").build();
        DiscoveryModule module = new DiscoveryModule(settings);
        module.addElectMasterService("custom", DummyMasterElectionService.class);
        assertBinding(module, ElectMasterService.class, DummyMasterElectionService.class);
        assertBinding(module, Discovery.class, ZenDiscovery.class);
    }

    public void testLoadUnregisteredMasterElectionService() {
        Settings settings = Settings.builder().put("node.local", false).
                put(DiscoveryModule.ZEN_MASTER_SERVICE_TYPE_KEY, "foobar").build();
        DiscoveryModule module = new DiscoveryModule(settings);
        module.addElectMasterService("custom", DummyMasterElectionService.class);
        assertBindingFailure(module, "Unknown master service type [foobar]");
    }

    public void testRegisterDefaults() {
        boolean local = randomBoolean();
        Settings settings = Settings.builder().put("node.local", local).build();
        DiscoveryModule module = new DiscoveryModule(settings);
        assertBinding(module, Discovery.class, local ? LocalDiscovery.class : ZenDiscovery.class);
    }

    public void testRegisterDiscovery() {
        boolean local = randomBoolean();
        Settings settings = Settings.builder().put("node.local", local).
                put(DiscoveryModule.DISCOVERY_TYPE_KEY, "custom").build();
        DiscoveryModule module = new DiscoveryModule(settings);
        module.addDiscoveryType("custom", DummyDisco.class);
        assertBinding(module, Discovery.class, DummyDisco.class);
    }


    public static class DummyDisco implements Discovery {


        @Override
        public DiscoveryNode localNode() {
            return null;
        }

        @Override
        public void addListener(InitialStateDiscoveryListener listener) {

        }

        @Override
        public void removeListener(InitialStateDiscoveryListener listener) {

        }

        @Override
        public String nodeDescription() {
            return null;
        }

        @Override
        public void setNodeService(@Nullable NodeService nodeService) {

        }

        @Override
        public void setRoutingService(RoutingService routingService) {

        }

        @Override
        public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {

        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public Discovery start() {
            return null;
        }

        @Override
        public Discovery stop() {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
