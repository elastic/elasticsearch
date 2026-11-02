/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * The lookup service plans the pre-join plan it receives with the lookup optimizers; an external source has no place
 * in it and nothing on that path authorizes or rebinds a dataset. A crafted lookup request carrying one must be
 * refused before the planners see it.
 */
public class LookupFromIndexExternalSourceIT extends AbstractEsqlIntegTestCase {

    public void testLookupPlanCarryingAnExternalSourceIsRefused() {
        String node = randomFrom(internalCluster().getNodeNames());
        LookupFromIndexService service = internalCluster().getInstance(TransportEsqlQueryAction.class, node).getLookupFromIndexService();
        List<Attribute> output = List.of(
            new FieldAttribute(Source.EMPTY, "key", new EsField("key", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://attacker/whatever/*.parquet",
            new SimpleSourceMetadata(output, "parquet", "s3://attacker/whatever/*.parquet"),
            output,
            FileList.UNRESOLVED,
            Map.of(),
            "secret_dataset"
        );
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Page inputPage = new Page(blockFactory.newConstantIntBlockWith(1, 1));
        try {
            LookupFromIndexService.LookupRequest request = new LookupFromIndexService.LookupRequest(
                "lookup-external-" + randomAlphaOfLength(8),
                new ShardId("lookup_idx", "uuid", 0),
                "lookup_idx",
                inputPage,
                null,
                List.<NamedExpression>of(output.get(0)),
                List.of(new MatchConfig("key", 0, DataType.INTEGER)),
                Source.EMPTY,
                new FragmentExec(relation),
                null,
                randomBoolean() ? "client-to-server" : null,
                null,
                false,
                TEST_CFG
            );
            CancellableTask task = new CancellableTask(
                randomNonNegativeLong(),
                "transport",
                LookupFromIndexService.LOOKUP_ACTION_NAME,
                "",
                TaskId.EMPTY_TASK_ID,
                Map.of()
            );
            PlainActionFuture<AbstractLookupService.LookupResponse> future = new PlainActionFuture<>();
            service.doLookup(request, task, future);
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> future.actionGet(DEFAULT_REQUEST_TIMEOUT)
            );
            assertThat(e.status(), is(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), containsString("external datasets cannot be read by a lookup join"));
        } finally {
            inputPage.releaseBlocks();
        }
    }
}
