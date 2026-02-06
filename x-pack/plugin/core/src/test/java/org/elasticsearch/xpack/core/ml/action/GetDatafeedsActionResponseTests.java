/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction.Response;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetDatafeedsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<DatafeedConfig> datafeedList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            datafeedList.add(DatafeedConfigTests.createRandomizedDatafeedConfig(randomAlphaOfLength(10)));
        }
        return new Response(new QueryPage<>(datafeedList, datafeedList.size(), DatafeedConfig.RESULTS_FIELD));
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }
    /**
         * Tests merging responses with the same datafeed ID but different running states,
         * where both states have a searchInterval with different start times.
         * The state with the more recent searchInterval (larger startMs value) should be selected.
         */
        public void testMergeWithDuplicateKeysAndDifferentSearchIntervals() {
            SearchInterval olderInterval = new SearchInterval(1000L, 2000L);
            SearchInterval newerInterval = new SearchInterval(3000L, 4000L);

            RunningState olderState = new RunningState(true, true, olderInterval);
            RunningState newerState = new RunningState(false, false, newerInterval);

            String datafeedId = "test-datafeed";
            Response response1 = Response.fromTaskAndState(datafeedId, olderState);
            Response response2 = Response.fromTaskAndState(datafeedId, newerState);

            Response mergedResponse = Response.fromResponses(java.util.List.of(response1, response2));

            assertEquals(newerState, mergedResponse.getRunningState(datafeedId).orElse(null));

            mergedResponse = Response.fromResponses(java.util.List.of(response2, response1));
            assertEquals(newerState, mergedResponse.getRunningState(datafeedId).orElse(null));
        }

        /**
         * Tests merging responses with the same datafeed ID but different running states,
         * where only one state has a searchInterval.
         * The state with the searchInterval should be selected, regardless of order.
         */
        public void testMergeWithDuplicateKeysWhenOnlyOneHasSearchInterval() {
            SearchInterval interval = new SearchInterval(1000L, 2000L);

            RunningState stateWithInterval = new RunningState(true, true, interval);
            RunningState stateWithoutInterval = new RunningState(false, false, null);

            String datafeedId = "test-datafeed";
            Response response1 = Response.fromTaskAndState(datafeedId, stateWithInterval);
            Response response2 = Response.fromTaskAndState(datafeedId, stateWithoutInterval);

            Response mergedResponse = Response.fromResponses(java.util.List.of(response1, response2));

            assertEquals(stateWithInterval, mergedResponse.getRunningState(datafeedId).orElse(null));

            mergedResponse = Response.fromResponses(java.util.List.of(response2, response1));
            assertEquals(stateWithInterval, mergedResponse.getRunningState(datafeedId).orElse(null));
        }

        /**
         * Tests merging responses with the same datafeed ID but different running states,
         * where neither state has a searchInterval.
         * In this case, the second state in the list should be selected.
         */
        public void testMergeWithDuplicateKeysWhenNeitherHasSearchInterval() {
            RunningState state1 = new RunningState(true, true, null);
            RunningState state2 = new RunningState(false, false, null);

            String datafeedId = "test-datafeed";
            Response response1 = Response.fromTaskAndState(datafeedId, state1);
            Response response2 = Response.fromTaskAndState(datafeedId, state2);

            Response mergedResponse = Response.fromResponses(java.util.List.of(response1, response2));

            assertEquals(state2, mergedResponse.getRunningState(datafeedId).orElse(null));
        }

}
