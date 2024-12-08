/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

public class MainResponseTests extends ESTestCase {
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
                    build.minWireCompatVersion(),
                    Build.minimumCompatString(IndexVersions.MINIMUM_COMPATIBLE)
                )
            ),
            Strings.toString(builder)
        );
    }
}
