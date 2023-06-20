/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Date;

public class MainResponseTests extends AbstractXContentSerializingTestCase<MainResponse> {

    @Override
    protected MainResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        TransportVersion transportVersion = TransportVersionUtils.randomVersion();
        Build build = new Build(Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(), version.toString());
        return new MainResponse(nodeName, version, transportVersion, clusterName, clusterUuid, build);
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
        Build build = new Build(current.type(), current.hash(), current.date(), current.isSnapshot(), current.qualifiedVersion());
        Version version = Version.CURRENT;
        TransportVersion transportVersion = TransportVersion.current();
        MainResponse response = new MainResponse("nodeName", version, transportVersion, new ClusterName("clusterName"), clusterUUID, build);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            XContentHelper.stripWhitespace(
                Strings.format(
                    """
                        {
                            "name": "nodeName",
                            "cluster_name": "clusterName",
                            "cluster_uuid": "%s",
                            "version": {
                                "number": "%s",
                                "build_flavor": "default",
                                "build_type": "%s",
                                "build_hash": "%s",
                                "build_date": "%s",
                                "build_snapshot": %s,
                                "lucene_version": "%s",
                                "minimum_wire_compatibility_version": "%s",
                                "minimum_index_compatibility_version": "%s",
                                "transport_version": "%s"
                            },
                            "tagline": "You Know, for Search"
                        }
                        """,
                    clusterUUID,
                    build.qualifiedVersion(),
                    current.type().displayName(),
                    current.hash(),
                    current.date(),
                    current.isSnapshot(),
                    version.luceneVersion().toString(),
                    version.minimumCompatibilityVersion().toString(),
                    version.minimumIndexCompatibilityVersion().toString(),
                    transportVersion.toString()
                )
            ),
            Strings.toString(builder)
        );
    }

    @Override
    protected MainResponse mutateInstance(MainResponse mutateInstance) {
        String clusterUuid = mutateInstance.getClusterUuid();
        Build build = mutateInstance.getBuild();
        Version version = mutateInstance.getVersion();
        TransportVersion transportVersion = mutateInstance.getTransportVersion();
        String nodeName = mutateInstance.getNodeName();
        ClusterName clusterName = mutateInstance.getClusterName();
        switch (randomIntBetween(0, 5)) {
            case 0 -> clusterUuid = clusterUuid + randomAlphaOfLength(5);
            case 1 -> nodeName = nodeName + randomAlphaOfLength(5);
            case 2 ->
                // toggle the snapshot flag of the original Build parameter
                build = new Build(Build.Type.UNKNOWN, build.hash(), build.date(), build.isSnapshot() == false, build.qualifiedVersion());
            case 3 -> version = randomValueOtherThan(version, () -> VersionUtils.randomVersion(random()));
            case 4 -> clusterName = new ClusterName(clusterName + randomAlphaOfLength(5));
            case 5 -> transportVersion = randomValueOtherThan(transportVersion, () -> TransportVersionUtils.randomVersion(random()));
        }
        return new MainResponse(nodeName, version, transportVersion, clusterName, clusterUuid, build);
    }
}
