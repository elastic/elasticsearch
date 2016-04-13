/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.junit.After;
import org.junit.Before;

import java.util.Locale;

import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.DAILY;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.HOURLY;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.MONTHLY;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.WEEKLY;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test checks to make sure that the index audit trail actually updates the mappings on startups
 */
public class IndexAuditTrailUpdateMappingTests extends ShieldIntegTestCase {
    private ThreadPool threadPool;
    private IndexAuditTrail auditor;

    @Before
    public void setup() {
        threadPool = new ThreadPool("index audit trail update mapping tests");
    }

    public void testMappingIsUpdated() throws Exception {
        // Setup
        IndexNameResolver.Rollover rollover = randomFrom(HOURLY, DAILY, WEEKLY, MONTHLY);
        Settings settings = Settings.builder().put("xpack.security.audit.index.rollover", rollover.name().toLowerCase(Locale.ENGLISH))
                .put("path.home", createTempDir()).build();
        Transport transport = mock(Transport.class);
        when(transport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { DummyTransportAddress.INSTANCE },
                DummyTransportAddress.INSTANCE));
        auditor = new IndexAuditTrail(settings, transport, Providers.of(internalClient()), threadPool,
                mock(ClusterService.class));

        // before starting we add an event
        auditor.authenticationFailed(new FakeRestRequest());
        IndexAuditTrail.Message message = auditor.peek();

        // resolve the index name and force create it
        final String indexName = IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, message.timestamp, rollover);
        client().admin().indices().prepareCreate(indexName).get();
        ensureGreen(indexName);

        // default mapping
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(indexName).get();

        try {
            // start the audit trail which should update the mappings since it is the master
            auditor.start(true);

            // get the updated mappings
            GetMappingsResponse updated = client().admin().indices().prepareGetMappings(indexName).get();
            assertThat(response.mappings().get(indexName).get(IndexAuditTrail.DOC_TYPE), nullValue());
            assertThat(updated.mappings().get(indexName).get(IndexAuditTrail.DOC_TYPE), notNullValue());
        } finally {
            auditor.close();
        }
    }

    @Override
    public void beforeIndexDeletion() {
        // no-op here because of the shard counter check
    }

    @After
    public void shutdown() {
        if (auditor != null) {
            auditor.stop();
        }
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }
}
