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

import com.google.api.services.compute.model.Instance;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class GceComputeServiceZeroNodeMock extends GceComputeServiceAbstractMock {

    @Override
    protected List<List<String>> getTags() {
        return new ArrayList();
    }

    @Override
    protected List<String> getZones() {
        return new ArrayList();
    }

    private final List<String> zoneList;

    @Override
    public Collection<Instance> instances() {
        logger.debug("get instances for zoneList [{}]", zoneList);

        List<List<Instance>> instanceListByZone = Lists.transform(zoneList, new Function<String, List<Instance>>() {
            @Override
            public List<Instance> apply(String zoneId) {
                // If we return null here we will get a trace as explained in issue 43
                return new ArrayList();
            }
        });

        //Collapse instances from all zones into one neat list
        List<Instance> instanceList = Lists.newArrayList(Iterables.concat(instanceListByZone));

        if (instanceList.size() == 0) {
            logger.warn("disabling GCE discovery. Can not get list of nodes");
        }

        return instanceList;
    }

    @Inject
    protected GceComputeServiceZeroNodeMock(Settings settings) {
        super(settings);
        String[] zoneList = settings.getAsArray(Fields.ZONE);
        this.zoneList = Lists.newArrayList(zoneList);
    }
}
