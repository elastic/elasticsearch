/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class EnterpriseSearchRestIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("entsearch-superuser", "entsearch-superuser-password", "superuser", false)
        .user("entsearch-admin", "entsearch-admin-password", "admin", false)
        .user("entsearch-user", "entsearch-user-password", "user", false)
        .user("entsearch-unprivileged", "entsearch-unprivileged-password", "unprivileged", false)
        .build();

    public EnterpriseSearchRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restAdminSettings() {
        final String value = basicAuthHeaderValue("entsearch-superuser", new SecureString("entsearch-superuser-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

    @Override
    protected Settings restClientSettings() {
        final String value = basicAuthHeaderValue("entsearch-admin", new SecureString("entsearch-admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
