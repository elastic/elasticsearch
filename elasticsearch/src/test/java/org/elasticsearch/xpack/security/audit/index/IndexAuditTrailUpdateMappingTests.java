/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import java.util.Locale;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail.State;
import org.junit.After;

import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.DAILY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.HOURLY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.MONTHLY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.WEEKLY;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test checks to make sure that the index audit trail actually updates the mappings on startups
 */
public class IndexAuditTrailUpdateMappingTests extends SecurityIntegTestCase {
    private ThreadPool threadPool;
    private IndexAuditTrail auditor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("index audit trail update mapping tests");
    }

    public void testMappingIsUpdated() throws Exception {
        // Setup
        IndexNameResolver.Rollover rollover = randomFrom(HOURLY, DAILY, WEEKLY, MONTHLY);
        Settings settings = Settings.builder().put("xpack.security.audit.index.rollover", rollover.name().toLowerCase(Locale.ENGLISH))
                .put("path.home", createTempDir()).build();
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(buildNewFakeTransportAddress().toString());
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        auditor = new IndexAuditTrail(settings, internalClient(), threadPool, clusterService);

        // before starting we add an event
        auditor.authenticationFailed(new FakeRestRequest());
        IndexAuditTrail.Message message = auditor.peek();

        // resolve the index name and force create it
        final String indexName = IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, message.timestamp, rollover);
        client().admin().indices().prepareCreate(indexName).get();
        ensureGreen(indexName);

        // default mapping
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(indexName).get();
        assertThat(response.mappings().get(indexName).get(IndexAuditTrail.DOC_TYPE), nullValue());

        // start the audit trail which should update the mappings since it is the master
        auditor.start(true);
        assertTrue(awaitBusy(() -> auditor.state() == State.STARTED));

        // get the updated mappings
        GetMappingsResponse updated = client().admin().indices().prepareGetMappings(indexName).get();
        assertThat(updated.mappings().get(indexName).get(IndexAuditTrail.DOC_TYPE), notNullValue());
    }

    @Override
    public void beforeIndexDeletion() {
        // no-op here because of the shard counter check
    }

    /**
     * We need to use our own method instead of {@link ESIntegTestCase#tearDown()} since checks are run against the cluster before the
     * teardown method is called by the {@link ESIntegTestCase#after()} method. If the {@link IndexAuditTrail} is still running and
     * indexing tests will randomly fail with failing to obtain shard locks for the audit indices.
     */
    @After
    public void cleanUp() throws Exception {
        if (auditor != null) {
            auditor.stop();
        }
        if (threadPool != null) {
            terminate(threadPool);
        }
    }
}
