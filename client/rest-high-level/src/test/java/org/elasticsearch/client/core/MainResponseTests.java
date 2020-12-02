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

package org.elasticsearch.client.core;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class MainResponseTests extends AbstractResponseTestCase<org.elasticsearch.action.main.MainResponse, MainResponse> {
    @Override
    protected org.elasticsearch.action.main.MainResponse createServerTestInstance(XContentType xContentType) {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Build build = new Build(
            Build.Flavor.UNKNOWN, Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(),
            version.toString()
        );
        return new org.elasticsearch.action.main.MainResponse(nodeName, version, clusterName, clusterUuid , build);
    }

    @Override
    protected MainResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return MainResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.main.MainResponse serverTestInstance, MainResponse clientInstance) {
        assertThat(serverTestInstance.ClusterName().value(), equalTo(clientInstance.ClusterName()));
        assertThat(serverTestInstance.ClusterUuid(), equalTo(clientInstance.ClusterUuid()));
        assertThat(serverTestInstance.NodeName(), equalTo(clientInstance.NodeName()));
        assertThat("You Know, for Search", equalTo(clientInstance.Tagline()));

        assertThat(serverTestInstance.Build().hash(), equalTo(clientInstance.Version().getBuildHash()));
        assertThat(serverTestInstance.Version().toString(), equalTo(clientInstance.Version().getNumber()));
        assertThat(serverTestInstance.Build().date(), equalTo(clientInstance.Version().getBuildDate()));
        assertThat(serverTestInstance.Build().flavor().displayName(), equalTo(clientInstance.Version().getBuildFlavor()));
        assertThat(serverTestInstance.Build().type().displayName(), equalTo(clientInstance.Version().getBuildType()));
        assertThat(serverTestInstance.Version().luceneVersion.toString(), equalTo(clientInstance.Version().getLuceneVersion()));
        assertThat(serverTestInstance.Version().minimumIndexCompatibilityVersion().toString(),
            equalTo(clientInstance.Version().getMinimumIndexCompatibilityVersion()));
        assertThat(serverTestInstance.Version().minimumCompatibilityVersion().toString(),
            equalTo(clientInstance.Version().getMinimumWireCompatibilityVersion()));
    }
}
