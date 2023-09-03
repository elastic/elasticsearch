/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.BuildUtils.newBuild;

public class MainResponseTests extends AbstractXContentSerializingTestCase<MainResponse> {

    @Override
    protected MainResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        IndexVersion indexVersion = IndexVersionUtils.randomVersion();
        Build build = newBuild(
            Build.current(),
            Map.of(
                "version",
                version.toString(),
                "minWireCompatVersion",
                version.minimumCompatibilityVersion().toString(),
                "minIndexCompatVersion",
                Build.minimumCompatString(IndexVersion.getMinimumCompatibleIndexVersion(indexVersion.id())),
                "displayString",
                Build.defaultDisplayString(Build.current().type(), Build.current().hash(), Build.current().date(), version.toString())
            )
        );
        return new MainResponse(nodeName, indexVersion.luceneVersion().toString(), clusterName, clusterUuid, build);
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
        final Build build = Build.current();
        Version version = Version.CURRENT;
        IndexVersion indexVersion = IndexVersion.current();
        MainResponse response = new MainResponse(
            "nodeName",
            indexVersion.luceneVersion().toString(),
            new ClusterName("clusterName"),
            clusterUUID,
            build
        );
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
                                "build_flavor": "%s",
                                "build_type": "%s",
                                "build_hash": "%s",
                                "build_date": "%s",
                                "build_snapshot": %s,
                                "lucene_version": "%s",
                                "minimum_wire_compatibility_version": "%s",
                                "minimum_index_compatibility_version": "%s"
                            },
                            "tagline": "You Know, for Search"
                        }
                        """,
                    clusterUUID,
                    build.qualifiedVersion(),
                    build.flavor(),
                    build.type().displayName(),
                    build.hash(),
                    build.date(),
                    build.isSnapshot(),
                    indexVersion.luceneVersion().toString(),
                    version.minimumCompatibilityVersion().toString(),
                    Build.minimumCompatString(IndexVersion.MINIMUM_COMPATIBLE)
                )
            ),
            Strings.toString(builder)
        );
    }

    @Override
    protected MainResponse mutateInstance(MainResponse mutateInstance) {
        String clusterUuid = mutateInstance.getClusterUuid();
        Build build = mutateInstance.getBuild();
        String luceneVersion = mutateInstance.getLuceneVersion();
        String nodeName = mutateInstance.getNodeName();
        ClusterName clusterName = mutateInstance.getClusterName();
        switch (randomIntBetween(0, 4)) {
            case 0 -> clusterUuid = clusterUuid + randomAlphaOfLength(5);
            case 1 -> nodeName = nodeName + randomAlphaOfLength(5);
            case 2 ->
                // toggle the snapshot flag of the original Build parameter
                build = newBuild(build, Map.of("isSnapshot", build.isSnapshot() == false));
            case 3 -> clusterName = new ClusterName(clusterName + randomAlphaOfLength(5));
            case 4 -> luceneVersion = randomValueOtherThan(
                luceneVersion,
                () -> IndexVersionUtils.randomVersion(random()).luceneVersion().toString()
            );
        }
        return new MainResponse(nodeName, luceneVersion, clusterName, clusterUuid, build);
    }
}
