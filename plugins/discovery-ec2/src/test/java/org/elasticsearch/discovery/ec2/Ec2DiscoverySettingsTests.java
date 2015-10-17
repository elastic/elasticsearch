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

import org.elasticsearch.cloud.aws.Ec2Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class Ec2DiscoverySettingsTests extends ESTestCase {

    public void testDiscoveryReady() {
        Settings settings = Settings.builder()
                .put("discovery.type", "ec2")
                .build();
        boolean discoveryReady = Ec2Module.isEc2DiscoveryActive(settings, logger);
        assertThat(discoveryReady, is(true));
    }

    public void testDiscoveryNotReady() {
        Settings settings = Settings.EMPTY;
        boolean discoveryReady = Ec2Module.isEc2DiscoveryActive(settings, logger);
        assertThat(discoveryReady, is(false));
    }

}
