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

package org.elasticsearch.action.main;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Date;

public class MainResponseTests extends AbstractSerializingTestCase<MainResponse> {

    @Override
    protected MainResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Build build = new Build(
            Build.Flavor.UNKNOWN, Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(),
            version.toString()
        );
        return new MainResponse(nodeName, version, clusterName, clusterUuid , build);
    }

    @Override
    protected Writeable.Reader<MainResponse> instanceReader() {
        return MainResponse::new;
    }

    @Override
    protected MainResponse doParseInstance(XContentParser parser) {
        return MainResponse.fromXContent(parser);
    }

    public void testToXContent() throws IOException {
        String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build current = Build.CURRENT;
        Build build = new Build(
            current.flavor(), current.type(), current.hash(), current.date(), current.isSnapshot(),
            current.getQualifiedVersion()
        );
        Version version = Version.CURRENT;
        MainResponse response = new MainResponse("nodeName", version, new ClusterName("clusterName"), clusterUUID, build);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{"
                + "\"name\":\"nodeName\","
                + "\"cluster_name\":\"clusterName\","
                + "\"cluster_uuid\":\"" + clusterUUID + "\","
                + "\"version\":{"
                    + "\"number\":\"" + build.getQualifiedVersion() + "\","
                    + "\"build_flavor\":\"" + current.flavor().displayName() + "\","
                    + "\"build_type\":\"" + current.type().displayName() + "\","
                    + "\"build_hash\":\"" + current.hash() + "\","
                    + "\"build_date\":\"" + current.date() + "\","
                    + "\"build_snapshot\":" + current.isSnapshot() + ","
                    + "\"lucene_version\":\"" + version.luceneVersion.toString() + "\","
                    + "\"minimum_wire_compatibility_version\":\"" + version.minimumCompatibilityVersion().toString() + "\","
                    + "\"minimum_index_compatibility_version\":\"" + version.minimumIndexCompatibilityVersion().toString() + "\"},"
                + "\"tagline\":\"You Know, for Search\""
          + "}", Strings.toString(builder));
    }

    @Override
    protected MainResponse mutateInstance(MainResponse mutateInstance) {
        String clusterUuid = mutateInstance.getClusterUuid();
        Build build = mutateInstance.getBuild();
        Version version = mutateInstance.getVersion();
        String nodeName = mutateInstance.getNodeName();
        ClusterName clusterName = mutateInstance.getClusterName();
        switch (randomIntBetween(0, 4)) {
            case 0:
                clusterUuid = clusterUuid + randomAlphaOfLength(5);
                break;
            case 1:
                nodeName = nodeName + randomAlphaOfLength(5);
                break;
            case 2:
                // toggle the snapshot flag of the original Build parameter
                build = new Build(
                    Build.Flavor.UNKNOWN, Build.Type.UNKNOWN, build.hash(), build.date(), !build.isSnapshot(),
                    build.getQualifiedVersion()
                );
                break;
            case 3:
                version = randomValueOtherThan(version, () -> VersionUtils.randomVersion(random()));
                break;
            case 4:
                clusterName = new ClusterName(clusterName + randomAlphaOfLength(5));
                break;
        }
        return new MainResponse(nodeName, version, clusterName, clusterUuid, build);
    }
}
