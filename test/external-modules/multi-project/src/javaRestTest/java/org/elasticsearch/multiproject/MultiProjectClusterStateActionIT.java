/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xcontent.ObjectPath;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;

/**
 * REST test for ensuring the /_cluster/state API is able to serialize multiple projects.
 */
public class MultiProjectClusterStateActionIT extends MultiProjectRestTestCase {

    @ClassRule
    public static ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return CLUSTER.getHttpAddresses();
    }

    public void testMultipleProjects() throws Exception {
        var response = client().performRequest(new Request("GET", "/_cluster/state?multi_project"));
        var projects = ObjectPath.<List<Map<String, ?>>>eval("metadata.projects", entityAsMap(response));
        assertNotNull(projects);
        assertEquals(1, projects.size());

        createProject("foo");
        response = client().performRequest(new Request("GET", "/_cluster/state?multi_project"));
        projects = ObjectPath.<List<Map<String, ?>>>eval("metadata.projects", entityAsMap(response));
        assertNotNull(projects);
        assertEquals(2, projects.size());
    }
}
