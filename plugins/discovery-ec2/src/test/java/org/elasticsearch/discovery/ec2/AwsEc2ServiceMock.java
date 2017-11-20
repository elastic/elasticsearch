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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Tag;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class AwsEc2ServiceMock extends AbstractLifecycleComponent implements AwsEc2Service {

    private int nodes;
    private List<List<Tag>> tagsList;
    private AmazonEC2 client;

    public AwsEc2ServiceMock(Settings settings, int nodes, List<List<Tag>> tagsList) {
        super(settings);
        this.nodes = nodes;
        this.tagsList = tagsList;
    }

    @Override
    public synchronized AmazonEC2 client() {
        if (client == null) {
            client = new AmazonEC2Mock(nodes, tagsList);
        }

        return client;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }
}
