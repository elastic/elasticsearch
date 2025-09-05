/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Assert;

import java.io.IOException;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class, TestContainersThreadFilter.class })
abstract class AbstractHaHdfsFailoverTestSuiteIT extends ESRestTestCase {

    abstract HdfsFixture getHdfsFixture();

    String securityCredentials() {
        return "";
    }

    public void testHAFailoverWithRepository() throws Exception {
        getHdfsFixture().setupHA();

        RestClient client = client();

        createRepository(client);

        // Get repository
        Response response = client.performRequest(new Request("GET", "/_snapshot/hdfs_ha_repo_read/_all"));
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        // Failover the namenode to the second.
        getHdfsFixture().failoverHDFS("nn1", "nn2");
        safeSleep(2000);
        // Get repository again
        response = client.performRequest(new Request("GET", "/_snapshot/hdfs_ha_repo_read/_all"));
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void createRepository(RestClient client) throws IOException {
        Request request = new Request("PUT", "/_snapshot/hdfs_ha_repo_read");
        request.setJsonEntity(Strings.format("""
            {
              "type": "hdfs",
              "settings": {
                "uri": "hdfs://ha-hdfs/",
                "path": "/user/elasticsearch/existing/readonly-repository",
                "readonly": "true",
                %s
                "conf.dfs.nameservices": "ha-hdfs",
                "conf.dfs.ha.namenodes.ha-hdfs": "nn1,nn2",
                "conf.dfs.namenode.rpc-address.ha-hdfs.nn1": "localhost:%s",
                "conf.dfs.namenode.rpc-address.ha-hdfs.nn2": "localhost:%s",
                "conf.dfs.client.failover.proxy.provider.ha-hdfs":\
                 "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
              }
            }""", securityCredentials(), getHdfsFixture().getPort(0), getHdfsFixture().getPort(1)));
        Response response = client.performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

}
