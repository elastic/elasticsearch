/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RepositoryDeprecationIT extends ESRestTestCase {

    private static final String INVALID_REPOSITORY_NAME = "invalid-repository";
    private static final String UNKNOWN_REPOSITORY_NAME = "unknown-repository";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("deprecation-plugin")
        .module("x-pack-deprecation")
        .module("x-pack-stack")
        .module("x-pack-ilm")
        .module("x-pack-ml")
        .module("mapper-extras")
        .module("wildcard")
        .module("ingest-common")
        .module("constant-keyword")
        .module("transform")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testInvalidRepositoryIsReportedByDeprecationInfoApi() throws Exception {
        assertOK(adminClient().performRequest(new Request("POST", "/_test_cluster/deprecation/create_test_repositories")));

        final var deprecations = assertOKAndCreateObjectPath(adminClient().performRequest(new Request("GET", "/_migration/deprecations")));
        assertDeprecationIssue(
            deprecations,
            INVALID_REPOSITORY_NAME,
            "Invalid repository",
            "This repository could not be initialized. Fix the repository configuration before upgrading."
        );
        assertDeprecationIssue(
            deprecations,
            UNKNOWN_REPOSITORY_NAME,
            "Unknown repository type",
            "This repository uses an unknown type. Ensure that all required plugins are installed before upgrading."
        );
    }

    private static void assertDeprecationIssue(ObjectPath deprecations, String repositoryName, String message, String details)
        throws IOException {
        final String issuePath = "repositories." + repositoryName;
        assertThat(deprecations.evaluate(issuePath), hasSize(1));
        assertThat(deprecations.evaluate(issuePath + ".0.level"), equalTo("critical"));
        assertThat(deprecations.evaluate(issuePath + ".0.message"), equalTo(message));
        assertThat(deprecations.evaluate(issuePath + ".0.details"), equalTo(details));
        assertThat(deprecations.evaluate(issuePath + ".0.resolve_during_rolling_upgrade"), equalTo(false));
    }
}
