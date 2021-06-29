/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        assertThat(serverTestInstance.getClusterName().value(), equalTo(clientInstance.getClusterName()));
        assertThat(serverTestInstance.getClusterUuid(), equalTo(clientInstance.getClusterUuid()));
        assertThat(serverTestInstance.getNodeName(), equalTo(clientInstance.getNodeName()));
        assertThat("You Know, for Search", equalTo(clientInstance.getTagline()));

        assertThat(serverTestInstance.getBuild().hash(), equalTo(clientInstance.getVersion().getBuildHash()));
        assertThat(serverTestInstance.getVersion().toString(), equalTo(clientInstance.getVersion().getNumber()));
        assertThat(serverTestInstance.getBuild().date(), equalTo(clientInstance.getVersion().getBuildDate()));
        assertThat(serverTestInstance.getBuild().flavor().displayName(), equalTo(clientInstance.getVersion().getBuildFlavor()));
        assertThat(serverTestInstance.getBuild().type().displayName(), equalTo(clientInstance.getVersion().getBuildType()));
        assertThat(serverTestInstance.getVersion().luceneVersion.toString(), equalTo(clientInstance.getVersion().getLuceneVersion()));
        assertThat(serverTestInstance.getVersion().minimumIndexCompatibilityVersion().toString(),
            equalTo(clientInstance.getVersion().getMinimumIndexCompatibilityVersion()));
        assertThat(serverTestInstance.getVersion().minimumCompatibilityVersion().toString(),
            equalTo(clientInstance.getVersion().getMinimumWireCompatibilityVersion()));
    }
}
