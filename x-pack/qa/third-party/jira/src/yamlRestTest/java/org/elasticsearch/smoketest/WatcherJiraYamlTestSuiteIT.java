/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;

/** Runs rest tests against external cluster */
public class WatcherJiraYamlTestSuiteIT extends AbstractWatcherThirdPartyYamlTestSuiteIT {

    @ClassRule
    public static ElasticsearchCluster cluster = baseClusterBuilder().setting(
        "xpack.notification.jira.account.test.issue_defaults.issuetype.name",
        "Bug"
    )
        .setting("xpack.notification.jira.account.test.issue_defaults.labels.0", "integration-tests")
        .setting("xpack.notification.jira.account.test.issue_defaults.project.key", System.getenv("jira_project"))
        .keystore("xpack.notification.jira.account.test.secure_url", System.getenv("jira_url"))
        .keystore("xpack.notification.jira.account.test.secure_user", System.getenv("jira_user"))
        .keystore("xpack.notification.jira.account.test.secure_password", System.getenv("jira_password"))
        .build();

    @Override
    protected ElasticsearchCluster getCluster() {
        return cluster;
    }

    public WatcherJiraYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }
}
