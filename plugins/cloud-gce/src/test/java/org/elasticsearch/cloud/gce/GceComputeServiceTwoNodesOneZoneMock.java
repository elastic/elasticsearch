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

package org.elasticsearch.cloud.gce;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.gce.mock.GceComputeServiceAbstractMock;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GceComputeServiceTwoNodesOneZoneMock extends GceComputeServiceAbstractMock {
    public static class Plugin extends AbstractPlugin {
        @Override
        public String name() {
            return "mock-compute-service";
        }
        @Override
        public String description() {
            return "a mock compute service for testing";
        }
        public void onModule(GceModule gceModule) {
            gceModule.computeServiceImpl = GceComputeServiceTwoNodesOneZoneMock.class;
        }
    }

    private static List<String> zones = Arrays.asList("us-central1-a", "us-central1-a");

    @Override
    protected List<List<String>> getTags() {
        return new ArrayList<>();
    }

    @Override
    protected List<String> getZones() {
        return zones;
    }

    @Inject
    protected GceComputeServiceTwoNodesOneZoneMock(Settings settings) {
        super(settings);
    }
}
