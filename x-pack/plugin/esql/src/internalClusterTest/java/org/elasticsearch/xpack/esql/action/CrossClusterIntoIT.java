/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class CrossClusterIntoIT extends AbstractCrossClusterTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            // The setting is rejected while the feature flag is off, which would fail the clusters before the tests can be skipped.
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        return settings.build();
    }

    @Before
    public void requiresSnapshot() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testRemoteSourceIsRejected() throws Exception {
        setupClusters(2);
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("dest-into"));

        for (String source : List.of("logs-*," + REMOTE_CLUSTER_1 + ":logs-*", REMOTE_CLUSTER_1 + ":logs-*")) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> runQuery("FROM " + source + " | KEEP v | INTO dest-into", null).close()
            );
            assertThat(e.getMessage(), containsString(Into.UNSUPPORTED_SOURCE));
        }
    }

    public void testRemoteThatTheQueryExcludesIsNotARemoteSource() throws Exception {
        assumeTrue("INTO writes through batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        setupClusters(2);
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate("dest-into")
                .setSettings(Settings.builder().put(IndexSettings.BATCH_INDEXING.getKey(), true))
        );

        String query = "FROM logs-*," + REMOTE_CLUSTER_1 + ":logs-*,-" + REMOTE_CLUSTER_1 + ":* | KEEP v | INTO dest-into";
        try (EsqlQueryResponse response = runQuery(query, null)) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            assertThat((Long) values.getFirst().getFirst(), greaterThan(0L));
        }
    }

    public void testOnlyAnUnavailableRemoteIsRejected() throws Exception {
        setupClusters(2);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("dest-into"));
        cluster(REMOTE_CLUSTER_1).close();

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM " + REMOTE_CLUSTER_1 + ":logs-* | KEEP v | INTO dest-into", null).close()
        );
        assertThat(e.getMessage(), containsString(Into.UNSUPPORTED_SOURCE));
    }

    public void testUnavailableRemoteIsNotSilentlySkipped() throws Exception {
        setupClusters(2);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("dest-into"));
        cluster(REMOTE_CLUSTER_1).close();

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*," + REMOTE_CLUSTER_1 + ":logs-* | KEEP v | INTO dest-into", null).close()
        );
        assertThat(e.getMessage(), containsString(Into.UNSUPPORTED_SOURCE));
    }
}
