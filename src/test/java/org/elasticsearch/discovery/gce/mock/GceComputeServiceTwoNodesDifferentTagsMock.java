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

package org.elasticsearch.discovery.gce.mock;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class GceComputeServiceTwoNodesDifferentTagsMock extends GceComputeServiceAbstractMock {
    private static List<ArrayList<String>> tags = Lists.newArrayList(
            Lists.newArrayList("dev"),
            Lists.newArrayList("elasticsearch","dev"));

    @Override
    protected List<ArrayList<String>> getTags() {
        return tags;
    }

    @Override
    protected List<String> getZones() {
        return Lists.newArrayList();
    }

    @Inject
    protected GceComputeServiceTwoNodesDifferentTagsMock(Settings settings) {
        super(settings);
    }
}
