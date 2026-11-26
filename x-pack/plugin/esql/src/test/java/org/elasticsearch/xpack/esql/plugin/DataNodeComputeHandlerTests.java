/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DataNodeComputeHandlerTests extends ESTestCase {

    /**
     * Documents that leave a data node name a context the fetch phase finds again. A request without fetch contexts
     * would name contexts that close with the request.
     */
    public void testDocumentsLeaveTheirNodeOnlyWithFetchContexts() {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        ExchangeSourceExec source = new ExchangeSourceExec(Source.EMPTY, List.of(doc), false);
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, source.output(), false, source);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateFetchContexts(plan, request(plan, null))
        );
        assertThat(e.getMessage(), equalTo("documents can only leave their node for a fetch, but the request kept no fetch contexts"));
        DataNodeComputeHandler.validateFetchContexts(plan, request(plan, randomPositiveTimeValue()));
    }

    /**
     * A node keeps no fetch context longer than its {@code search.max_keep_alive}, also when the coordinator asks for a
     * longer keep-alive.
     */
    public void testTheReaderContextKeepAliveStaysWithinTheMaxKeepAliveOfTheNode() {
        TimeValue maxKeepAlive = TimeValue.timeValueMinutes(between(1, 60));
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(SearchService.MAX_KEEPALIVE_SETTING.getKey(), maxKeepAlive).build(),
            Set.of(SearchService.MAX_KEEPALIVE_SETTING)
        );
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(), false)
        );

        TimeValue shorter = TimeValue.timeValueMillis(between(1, (int) maxKeepAlive.millis()));
        assertThat(DataNodeComputeHandler.readerContextKeepAlive(request(plan, shorter), clusterSettings), equalTo(shorter));
        TimeValue longer = TimeValue.timeValueMillis(maxKeepAlive.millis() + between(1, 3_600_000));
        assertThat(DataNodeComputeHandler.readerContextKeepAlive(request(plan, longer), clusterSettings), equalTo(maxKeepAlive));
    }

    private static DataNodeRequest request(ExchangeSinkExec plan, @Nullable TimeValue readerContextKeepAlive) {
        return new DataNodeRequest(
            "session",
            TaskId.EMPTY_TASK_ID,
            EsqlTestUtils.TEST_CFG,
            "",
            List.of(),
            Map.of(),
            plan,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            true,
            false,
            List.of(),
            0L,
            readerContextKeepAlive
        );
    }
}
