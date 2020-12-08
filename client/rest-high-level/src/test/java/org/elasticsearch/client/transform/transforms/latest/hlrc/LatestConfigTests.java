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

package org.elasticsearch.client.transform.transforms.latest.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LatestConfigTests
        extends AbstractResponseTestCase<LatestConfig, org.elasticsearch.client.transform.transforms.latest.LatestConfig> {

    public static LatestConfig randomLatestConfig() {
        return new LatestConfig(randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10)), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected LatestConfig createServerTestInstance(XContentType xContentType) {
        return randomLatestConfig();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.latest.LatestConfig doParseToClientInstance(XContentParser parser)
        throws IOException {
        return org.elasticsearch.client.transform.transforms.latest.LatestConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        LatestConfig serverTestInstance,
        org.elasticsearch.client.transform.transforms.latest.LatestConfig clientInstance
    ) {
        assertThat(serverTestInstance.getUniqueKey(), is(equalTo(clientInstance.getUniqueKey())));
        assertThat(serverTestInstance.getSort(), is(equalTo(clientInstance.getSort())));
    }
}
