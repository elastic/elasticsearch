/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class GetAliasesRaceIntegTests extends SecurityIntegTestCase {

    @Override
    protected String configUsers() {
        final String hash = new String(getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() + "race_user:" + hash + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "race_role:race_user\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """

            race_role:
              indices:
                - names: '*'
                  privileges: [ all ]
            """;
    }

    public void testGetAliasesFailsWhenConcurrentAliasRemoval() throws Exception {
        CopyOnWriteArrayList<Exception> failures = runAliasRace(null);

        assertFalse("Expected at least one IndexNotFoundException from the reader thread", failures.isEmpty());
        for (Exception e : failures) {
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("churn_alias_"));
        }
    }

    public void testGetAliasesWithIgnoreUnavailableSurvivesConcurrentAliasRemoval() throws Exception {
        IndicesOptions withIgnoreUnavailable = IndicesOptions.builder(GetAliasesRequest.DEFAULT_INDICES_OPTIONS)
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .build();

        CopyOnWriteArrayList<Exception> failures = runAliasRace(withIgnoreUnavailable);

        assertTrue("ignore_unavailable=true should prevent IndexNotFoundException, but got: " + failures, failures.isEmpty());
    }

    private CopyOnWriteArrayList<Exception> runAliasRace(IndicesOptions readerOptions) throws Exception {
        Map<String, String> authHeaders = Collections.singletonMap(
            BASIC_AUTH_HEADER,
            basicAuthHeaderValue("race_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        Client authedClient = client(internalCluster().getMasterName()).filterWithHeader(authHeaders);

        assertAcked(indicesAdmin().prepareCreate("target_index").addAlias(new Alias("target_alias")));
        for (int i = 0; i < 5; i++) {
            assertAcked(indicesAdmin().prepareCreate("churn_index_" + i).addAlias(new Alias("churn_alias_" + i)));
        }

        AtomicBoolean stop = new AtomicBoolean(false);
        CopyOnWriteArrayList<Exception> failures = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);

        Thread reader = new Thread(() -> {
            try {
                while (stop.get() == false) {
                    try {
                        GetAliasesRequest req = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "target_alias");
                        if (readerOptions != null) {
                            req.indicesOptions(readerOptions);
                        }
                        authedClient.execute(GetAliasesAction.INSTANCE, req).actionGet();
                    } catch (IndexNotFoundException e) {
                        failures.add(e);
                    }
                }
            } finally {
                latch.countDown();
            }
        });

        Thread writer = new Thread(() -> {
            try {
                int iter = 0;
                while (stop.get() == false) {
                    int idx = iter % 5;
                    try {
                        authedClient.admin()
                            .indices()
                            .aliases(
                                new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                                    IndicesAliasesRequest.AliasActions.remove().index("churn_index_" + idx).alias("churn_alias_" + idx)
                                )
                            )
                            .actionGet();
                        authedClient.admin()
                            .indices()
                            .aliases(
                                new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                                    IndicesAliasesRequest.AliasActions.add().index("churn_index_" + idx).alias("churn_alias_" + idx)
                                )
                            )
                            .actionGet();
                    } catch (Exception e) {
                        // alias may already be removed/added; ignore
                    }
                    iter++;
                }
            } finally {
                latch.countDown();
            }
        });

        reader.start();
        writer.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        stop.set(true);
        latch.await(10, TimeUnit.SECONDS);

        return failures;
    }
}
