/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import com.google.common.base.Predicate;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.junit.Test;

@ClusterScope(scope = Scope.TEST, randomDynamicTemplates = false)
public class IndexAuditTrailEnabledTests extends ShieldIntegrationTest {

    IndexNameResolver.Rollover rollover = randomFrom(IndexNameResolver.Rollover.values());

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal));
        builder.put("shield.audit.enabled", true);
        if (randomBoolean()) {
            builder.putArray("shield.audit.outputs", LoggingAuditTrail.NAME, IndexAuditTrail.NAME);
        } else {
            builder.putArray("shield.audit.outputs", IndexAuditTrail.NAME);
        }
        builder.put(IndexAuditTrail.ROLLOVER_SETTING, rollover);

        return builder.build();
    }

    @Override
    public void beforeIndexDeletion() {
        // For this test, this is a NO-OP because the index audit trail will continue to capture events and index after
        // the tests have completed. The default implementation of this method expects that nothing is performing operations
        // after the test has completed
    }

    @Test
    public void testIndexAuditTrailIndexExists() throws Exception {
        awaitIndexCreation();
    }

    void awaitIndexCreation() throws Exception {
        IndexNameResolver resolver = new IndexNameResolver();
        final String indexName = resolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, System.currentTimeMillis(), rollover);
        boolean success = awaitBusy(new Predicate<Void>() {
            @Override
            public boolean apply(Void o) {
                try {
                    ExistsResponse response =
                            client().prepareExists(indexName).execute().actionGet();
                    return response.exists();
                } catch (Exception e) {
                    return false;
                }
            }
        });

        if (!success) {
            fail("index [" + indexName + "] was not created");
        }
    }
}
