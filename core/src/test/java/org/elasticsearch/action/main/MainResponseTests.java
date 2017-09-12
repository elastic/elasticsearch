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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class MainResponseTests extends AbstractStreamableXContentTestCase<MainResponse> {

    @Override
    protected MainResponse getExpectedFromXContent(MainResponse testInstance) {
        // we cannot recreate the "available" flag from xContent, but should be "true" if request came through
        testInstance.available = true;
        return testInstance;
    }

    @Override
    protected MainResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        Build build = new Build(randomAlphaOfLength(8), new Date(randomNonNegativeLong()).toString(), randomBoolean());
        Version version = VersionUtils.randomVersion(random());
        boolean available = randomBoolean();
        return new MainResponse(nodeName, version, clusterName, clusterUuid , build, available);
    }

    @Override
    protected MainResponse createBlankInstance() {
        return new MainResponse();
    }

    @Override
    protected MainResponse doParseInstance(XContentParser parser) {
        return MainResponse.fromXContent(parser);
    }

    public void testToXContent() throws IOException {
        String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        Build build = new Build(Build.CURRENT.shortHash(), Build.CURRENT.date(), Build.CURRENT.isSnapshot());
        Version version = Version.CURRENT;
        MainResponse response = new MainResponse("nodeName", version, new ClusterName("clusterName"), clusterUUID, build, true);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{"
                + "\"name\":\"nodeName\","
                + "\"cluster_name\":\"clusterName\","
                + "\"cluster_uuid\":\"" + clusterUUID + "\","
                + "\"version\":{"
                    + "\"number\":\"" + version.toString() + "\","
                    + "\"build_hash\":\"" + Build.CURRENT.shortHash() + "\","
                    + "\"build_date\":\"" + Build.CURRENT.date() + "\","
                    + "\"build_snapshot\":" + Build.CURRENT.isSnapshot() + ","
                    + "\"lucene_version\":\"" + version.luceneVersion.toString() + "\","
                    + "\"minimum_wire_compatibility_version\":\"" + version.minimumCompatibilityVersion().toString() + "\","
                    + "\"minimum_index_compatibility_version\":\"" + version.minimumIndexCompatibilityVersion().toString() + "\"},"
                + "\"tagline\":\"You Know, for Search\""
          + "}", builder.string());
    }

    //TODO this should be removed and the metehod from AbstractStreamableTestCase should be
    //used instead once https://github.com/elastic/elasticsearch/pull/25910 goes in
    public void testEqualsAndHashcode() {
        MainResponse original = createTestInstance();
        checkEqualsAndHashCode(original, MainResponseTests::copy, MainResponseTests::mutate);
    }

    private static MainResponse copy(MainResponse o) {
        return new MainResponse(o.getNodeName(), o.getVersion(), o.getClusterName(), o.getClusterUuid(), o.getBuild(), o.isAvailable());
    }

    private static MainResponse mutate(MainResponse o) {
        String clusterUuid = o.getClusterUuid();
        boolean available = o.isAvailable();
        Build build = o.getBuild();
        Version version = o.getVersion();
        String nodeName = o.getNodeName();
        ClusterName clusterName = o.getClusterName();
        switch (randomIntBetween(0, 5)) {
        case 0:
            clusterUuid = clusterUuid + randomAlphaOfLength(5);
            break;
        case 1:
            nodeName = nodeName + randomAlphaOfLength(5);
            break;
        case 2:
            available = !available;
            break;
        case 3:
            // toggle the snapshot flag of the original Build parameter
            build = new Build(build.shortHash(), build.date(), !build.isSnapshot());
            break;
        case 4:
            version = randomValueOtherThan(version, () -> VersionUtils.randomVersion(random()));
            break;
        case 5:
            clusterName = new ClusterName(clusterName + randomAlphaOfLength(5));
            break;
        }
        return new MainResponse(nodeName, version, clusterName, clusterUuid, build, available);
    }
}
