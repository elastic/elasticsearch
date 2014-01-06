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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.cloud.azure.Instance;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.Set;

/**
 * Mock Azure API with a single started node
 */
public class AzureComputeServiceSimpleMock extends AzureComputeServiceAbstractMock {

    @Inject
    protected AzureComputeServiceSimpleMock(Settings settings) {
        super(settings);
    }

    @Override
    public Set<Instance> instances() {
        Set<Instance> instances = new HashSet<Instance>();
        Instance azureHost = new Instance();
        azureHost.setPrivateIp("127.0.0.1");
        instances.add(azureHost);

        return instances;
    }
}
