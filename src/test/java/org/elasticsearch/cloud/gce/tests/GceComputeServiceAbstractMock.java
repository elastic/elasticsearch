/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cloud.gce.tests;

import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Tags;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public abstract class GceComputeServiceAbstractMock extends AbstractLifecycleComponent<GceComputeServiceAbstractMock>
    implements GceComputeService {

    protected abstract List<ArrayList<String>> getTags();

    protected GceComputeServiceAbstractMock(Settings settings) {
        super(settings);
        logger.debug("starting GCE Api Mock with {} nodes:", getTags().size());
        for (List<String> tags : getTags()) {
            logger.debug(" - {}", tags);
        }
    }

    @Override
    public Collection<Instance> instances() {
        Collection<Instance> instances = new ArrayList<Instance>();

        // For each instance (item of tags)
        for (List<String> tags : getTags()) {
            Instance instance = new Instance();
            instance.setName("Mock Node " + tags);
            instance.setMachineType("Mock Type machine");
            instance.setImage("mock-image-type");
            instance.setStatus("STARTED");
            Tags instanceTags = new Tags();
            instanceTags.setItems(tags);
            instance.setTags(instanceTags);
            NetworkInterface networkInterface = new NetworkInterface();
            networkInterface.setNetworkIP("localhost");
            List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
            networkInterfaces.add(networkInterface);
            instance.setNetworkInterfaces(networkInterfaces);

            instances.add(instance);
        }


        return instances;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }
}
