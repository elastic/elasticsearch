/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.Before;
import org.junit.ClassRule;

public class WatcherYamlRestWithSecurityIT extends WatcherYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = watcherClusterSpec().setting("xpack.security.enabled", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(WATCHER_USER, TEST_PASSWORD, "watcher_manager", false)
        .user("x_pack_rest_user", TEST_PASSWORD, "watcher_manager", false)
        // settings to test settings filtering on
        .setting("xpack.notification.email.account._email.smtp.host", "host.domain")
        .setting("xpack.notification.email.account._email.smtp.port", "587")
        .setting("xpack.notification.email.account._email.smtp.user", "_user")
        .keystore("xpack.notification.email.account._email.smtp.secure_password", "_passwd")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public WatcherYamlRestWithSecurityIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters("security");
    }

    @Before
    public void beforeTest() throws Exception {
        // create one document in this index, so we can test in the YAML tests, that the index cannot be accessed
        Request request = new Request("PUT", "/index_not_allowed_to_read/_doc/1");
        request.setJsonEntity("{\"foo\":\"bar\"}");
        adminClient().performRequest(request);
    }
}
