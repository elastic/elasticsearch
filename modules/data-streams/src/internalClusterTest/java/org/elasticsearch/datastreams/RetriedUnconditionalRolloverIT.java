/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.datastreams.DataStreamIT.putComposableIndexTemplate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Data stream variant of {@code RolloverIT#testRetriedUnconditionalRolloverWithExpectedSourceIndexRollsOverAliasOnce}, see
 * <a href="https://github.com/elastic/elasticsearch/issues/166865">#166865</a>. ILM sends an unconditional rollover, and if the master
 * fails over while it is in flight, the {@code TransportMasterNodeAction} retries the very same request on the new master. Replaying the
 * request here is exactly what that retry does: because it names the source index it expects to roll over, the replay must not roll over
 * the index that the first attempt created.
 */
public class RetriedUnconditionalRolloverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // MapperExtrasPlugin provides the match_only_text field used by the failure store mappings
        return List.of(DataStreamsPlugin.class, MapperExtrasPlugin.class);
    }

    public void testRetriedRolloverWithExpectedSourceIndexRollsOverDataStreamOnce() throws Exception {
        assertRetriedRolloverWithExpectedSourceIndexRollsOverOnce(false);
    }

    public void testRetriedRolloverWithExpectedSourceIndexRollsOverFailureStoreOnce() throws Exception {
        assertRetriedRolloverWithExpectedSourceIndexRollsOverOnce(true);
    }

    private void assertRetriedRolloverWithExpectedSourceIndexRollsOverOnce(boolean targetFailureStore) throws Exception {
        // given a data stream whose (failure store) write index is known
        final String dataStreamName = "logs-retried-rollover";
        putComposableIndexTemplate("retried-rollover", List.of(dataStreamName + "*"), targetFailureStore);
        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
        ).get();
        final String rolloverTarget = targetFailureStore
            ? IndexNameExpressionResolver.combineSelector(dataStreamName, IndexComponentSelector.FAILURES)
            : dataStreamName;
        if (targetFailureStore) {
            // the failure store is created lazily, so initialize it to get a failure store write index to roll over
            assertTrue(indicesAdmin().rolloverIndex(new RolloverRequest(rolloverTarget, null)).get().isRolledOver());
        }
        final String firstWriteIndex = getWriteIndex(dataStreamName, targetFailureStore);
        final RolloverRequest rolloverRequest = new RolloverRequest(rolloverTarget, null);
        rolloverRequest.setExpectedSourceIndex(firstWriteIndex);

        // when the same unconditional rollover request is executed twice
        final RolloverResponse firstResponse = indicesAdmin().rolloverIndex(rolloverRequest).get();
        final RolloverResponse retryResponse = indicesAdmin().rolloverIndex(rolloverRequest).get();

        // then only the first one rolls over
        assertThat(firstResponse.isRolledOver(), is(true));
        assertThat(firstResponse.getOldIndex(), equalTo(firstWriteIndex));
        assertThat(retryResponse.isRolledOver(), is(false));
        assertThat(retryResponse.getOldIndex(), equalTo(firstResponse.getNewIndex()));
        assertThat(getWriteIndex(dataStreamName, targetFailureStore), equalTo(firstResponse.getNewIndex()));
    }

    private static String getWriteIndex(String dataStreamName, boolean failureStore) {
        final DataStream dataStream = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .dataStreams()
            .get(dataStreamName);
        return failureStore ? dataStream.getWriteFailureIndex().getName() : dataStream.getWriteIndex().getName();
    }
}
