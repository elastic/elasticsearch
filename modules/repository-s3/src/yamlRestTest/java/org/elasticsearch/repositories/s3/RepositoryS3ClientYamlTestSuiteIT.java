/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;


@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakAction({ ThreadLeakAction.Action.WARN, ThreadLeakAction.Action.INTERRUPT })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RepositoryS3ClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public RepositoryS3ClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    private static final String s3PermanentAccessKey = "s3_test_access_key";//System.getenv("amazon_s3_access_key");
    private static final String s3PermanentSecretKey = "s3_test_secret_key"; // System.getenv("amazon_s3_secret_key");
    private static final String s3TemporaryAccessKey = "session_token_access_key";//System.getenv("amazon_s3_access_key_temporary");
    private static final String s3TemporarySecretKey = "session_token_secret_key"; //System.getenv("amazon_s3_secret_key_temporary");
    private static final String s3TemporarySessionToken = "session_token";// System.getenv("amazon_s3_session_token_temporary");

    @ClassRule
    public static ComposeContainer environment =
        new ComposeContainer(new File("/Users/rene/dev/elastic/elasticsearch/test/fixtures/s3-fixture/docker-compose.yml"))
            .withExposedService("s3-fixture", 80, Wait.forListeningPort())
            .withExposedService("s3-fixture-with-session-token", 80, Wait.forListeningPort())
            .withExposedService("s3-fixture-with-ec2", 80, Wait.forListeningPort())
            .withLocalCompose(true)
        /*.withLogConsumer("s3-fixture", (log) -> {
                System.out.println("s3-fixture: " + log.getUtf8String());
            }).withLogConsumer("s3-fixture-with-session-token", (log) -> {
                System.out.println("s3-fixture-with-session-token: " + log.getUtf8String());
            }).withLogConsumer("s3-fixture-with-ec2", (log) -> {
                System.out.println("s3-fixture-with-ec2: " + log.getUtf8String());
            })*/
        ;
            /*.withLogConsumer("s3-fixture", (log) -> {
                System.out.println("s3-fixture-with-ec2: " + log.getUtf8String());
            }).withLogConsumer("s3-fixture-with-ec2", (log) -> {
                System.out.println("s3-fixture-with-session-token: " + log.getUtf8String());
            });*/


    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.integration_test_permanent.access_key", s3PermanentAccessKey)
        .keystore("s3.client.integration_test_permanent.secret_key", s3PermanentSecretKey)
        .keystore("s3.client.integration_test_temporary.access_key", s3TemporaryAccessKey)
        .keystore("s3.client.integration_test_temporary.secret_key", s3TemporarySecretKey)
        .keystore("s3.client.integration_test_temporary.session_token", s3TemporarySessionToken)
        .setting("s3.client.integration_test_permanent.endpoint", () -> ("http://127.0.0.1:" + environment.getServicePort("s3-fixture", 80)))
        .setting("s3.client.integration_test_temporary.endpoint", () -> ("http://127.0.0.1:" + environment.getServicePort("s3-fixture-with-session-token", 80)))
        .setting("s3.client.integration_test_ec2.endpoint", () -> ("http://127.0.0.1:" + environment.getServicePort("s3-fixture-with-ec2", 80)))
        .systemProperty("com.amazonaws.sdk.ec2MetadataServiceEndpointOverride", () -> ("http://127.0.0.1:" + environment.getServicePort("s3-fixture-with-ec2", 80)))
        .build();


    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(environment).around(cluster);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
