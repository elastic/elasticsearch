/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.action.GetDatafeedsAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.datafeed.Datafeed;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetDatafeedsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<DatafeedConfig> datafeedList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String datafeedId = DatafeedConfigTests.randomValidDatafeedId();
            String jobId = randomAsciiOfLength(10);
            DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
            datafeedConfig.setIndexes(randomSubsetOf(2, Arrays.asList("index-1", "index-2", "index-3")));
            datafeedConfig.setTypes(randomSubsetOf(2, Arrays.asList("type-1", "type-2", "type-3")));
            datafeedConfig.setFrequency(randomNonNegativeLong());
            datafeedConfig.setQueryDelay(randomNonNegativeLong());
            if (randomBoolean()) {
                datafeedConfig.setQuery(QueryBuilders.termQuery(randomAsciiOfLength(10), randomAsciiOfLength(10)));
            }
            int scriptsSize = randomInt(3);
            if (randomBoolean()) {
                List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
                for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                    scriptFields.add(new SearchSourceBuilder.ScriptField(randomAsciiOfLength(10), new Script(randomAsciiOfLength(10)),
                            randomBoolean()));
                }
                datafeedConfig.setScriptFields(scriptFields);
            }
            if (randomBoolean()) {
                datafeedConfig.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean() && scriptsSize == 0) {
                AggregatorFactories.Builder aggsBuilder = new AggregatorFactories.Builder();
                aggsBuilder.addAggregator(AggregationBuilders.avg(randomAsciiOfLength(10)));
                datafeedConfig.setAggregations(aggsBuilder);
            }

            datafeedList.add(datafeedConfig.build());
        }

        result = new Response(new QueryPage<>(datafeedList, datafeedList.size(), Datafeed.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
