/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.main;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Date;

public class MainRestResponseTests extends AbstractXContentTestCase<MainRestResponse> {

    @Override
    protected MainRestResponse createTestInstance() {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Build build = new Build(Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(), version.toString());
        return new MainRestResponse(nodeName, version, clusterName, clusterUuid, build);
    }

    @Override
    protected MainRestResponse doParseInstance(XContentParser parser) {
        return MainRestResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testToXContent() throws IOException {
        String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Build current = Build.CURRENT;
        Build build = new Build(current.type(), current.hash(), current.date(), current.isSnapshot(), current.qualifiedVersion());
        Version version = Version.CURRENT;
        MainRestResponse response = new MainRestResponse("nodeName", version, new ClusterName("clusterName"), clusterUUID, build);
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
                                "minimum_index_compatibility_version": "%s"
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
                    version.luceneVersion.toString(),
                    version.minimumCompatibilityVersion().toString(),
                    version.minimumIndexCompatibilityVersion().toString()
                )
            ),
            Strings.toString(builder)
        );
    }

}
