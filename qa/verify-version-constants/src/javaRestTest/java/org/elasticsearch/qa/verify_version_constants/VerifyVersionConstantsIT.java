/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.qa.verify_version_constants;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class VerifyVersionConstantsIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(System.getProperty("tests.cluster_version"))
        .setting("xpack.security.enabled", "false")
        .build();

    public void testLuceneVersionConstant() throws IOException, ParseException {
        Response response = client().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);

        String luceneVersionString = objectPath.evaluate("version.lucene_version").toString();
        org.apache.lucene.util.Version luceneVersion = org.apache.lucene.util.Version.parse(luceneVersionString);

        IndexVersion indexVersion = getIndexVersion();
        assertThat(indexVersion.luceneVersion(), equalTo(luceneVersion));
    }

    private IndexVersion getIndexVersion() throws IOException {
        IndexVersion indexVersion = null;

        Request request = new Request("GET", "_nodes");
        request.addParameter("filter_path", "nodes.*.index_version,nodes.*.name");
        Response response = client().performRequest(request);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        for (String id : nodeMap.keySet()) {
            Number ix = objectPath.evaluate("nodes." + id + ".index_version");
            IndexVersion version;
            if (ix != null) {
                version = IndexVersion.fromId(ix.intValue());
            } else {
                // it doesn't have index version (pre 8.11) - just infer it from the release version
                version = parseLegacyVersion(System.getProperty("tests.cluster_version")).map(x -> IndexVersion.fromId(x.id()))
                    .orElse(IndexVersions.MINIMUM_COMPATIBLE);
            }

            if (indexVersion == null) {
                indexVersion = version;
            } else {
                String name = objectPath.evaluate("nodes." + id + ".name");
                assertThat("Node " + name + " has a different index version to other nodes", version, Matchers.equalTo(indexVersion));
            }
        }

        assertThat("Index version could not be read", indexVersion, notNullValue());
        return indexVersion;
    }

    @Override
    public boolean preserveClusterUponCompletion() {
        /*
         * We don't perform any writes to the cluster so there won't be anything
         * to clean up. Also, our cleanup code is really only compatible with
         * *write* compatible versions but this runs with *index* compatible
         * versions.
         */
        return true;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
