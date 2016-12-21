/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.prelert.action.GetSchedulersAction.Response;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.scheduler.Scheduler;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfigTests;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetSchedulersActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<SchedulerConfig> schedulerList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String schedulerId = SchedulerConfigTests.randomValidSchedulerId();
            String jobId = randomAsciiOfLength(10);
            SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(schedulerId, jobId);
            schedulerConfig.setIndexes(randomSubsetOf(2, Arrays.asList("index-1", "index-2", "index-3")));
            schedulerConfig.setTypes(randomSubsetOf(2, Arrays.asList("type-1", "type-2", "type-3")));
            schedulerConfig.setFrequency(randomPositiveLong());
            schedulerConfig.setQueryDelay(randomPositiveLong());
            if (randomBoolean()) {
                schedulerConfig.setQuery(QueryBuilders.termQuery(randomAsciiOfLength(10), randomAsciiOfLength(10)));
            }
            if (randomBoolean()) {
                int scriptsSize = randomInt(3);
                List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
                for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                    scriptFields.add(new SearchSourceBuilder.ScriptField(randomAsciiOfLength(10), new Script(randomAsciiOfLength(10)),
                            randomBoolean()));
                }
                schedulerConfig.setScriptFields(scriptFields);
            }
            if (randomBoolean()) {
                schedulerConfig.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
            }
            if (randomBoolean()) {
                AggregatorFactories.Builder aggsBuilder = new AggregatorFactories.Builder();
                aggsBuilder.addAggregator(AggregationBuilders.avg(randomAsciiOfLength(10)));
                schedulerConfig.setAggregations(aggsBuilder);
            }

            schedulerList.add(schedulerConfig.build());
        }

        result = new Response(new QueryPage<>(schedulerList, schedulerList.size(), Scheduler.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
