/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.auditor;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class NotificationsIndexIT extends TransformSingleNodeTestCase {
    public void testAliasCreated() throws Exception {
        // Auditing a notification should create the .transform-notifications-000002
        // index and the write alias
        createNotification(true);

        assertBusy(() -> {
            assertNotificationsIndexExists();
            assertNotificationsWriteAliasCreated();
        });
    }

    private void assertNotificationsIndexExists() {
        GetIndexResponse getIndexResponse = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(TransformInternalIndexConstants.AUDIT_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .get();
        assertThat(Arrays.asList(getIndexResponse.getIndices()), contains(TransformInternalIndexConstants.AUDIT_INDEX));
    }

    private void assertNotificationsWriteAliasCreated() {
        Map<String, List<AliasMetadata>> aliases = indicesAdmin().prepareGetAliases(
            TimeValue.timeValueSeconds(10L),
            TransformInternalIndexConstants.AUDIT_INDEX_WRITE_ALIAS
        ).setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN).get().getAliases();
        assertThat(aliases.size(), is(1));
        List<AliasMetadata> indexAliases = aliases.get(TransformInternalIndexConstants.AUDIT_INDEX);
        assertNotNull(aliases.toString(), indexAliases);
        assertThat(indexAliases.size(), is(1));
        var writeAlias = indexAliases.get(0);
        assertThat(writeAlias.alias(), is(TransformInternalIndexConstants.AUDIT_INDEX_WRITE_ALIAS));
        assertThat("notification write alias should be hidden but is not: " + aliases, writeAlias.isHidden(), is(true));
    }

    private void createNotification(boolean includeNodeInfo) {
        var clusterService = getInstanceFromNode(ClusterService.class);
        TransformAuditor auditor = new TransformAuditor(
            client(),
            clusterService.getNodeName(),
            clusterService,
            TestIndexNameExpressionResolver.newInstance(),
            includeNodeInfo
        );
        auditor.info("whatever", "blah");
    }
}
