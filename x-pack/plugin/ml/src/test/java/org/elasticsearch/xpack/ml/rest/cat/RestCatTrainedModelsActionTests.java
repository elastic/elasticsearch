/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.cat;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsActionResponseTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStats;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RestCatTrainedModelsActionTests extends ESTestCase {

    public void testBuildTableAccumulatedStats() {
        var action = new RestCatTrainedModelsAction(true);

        // GetTrainedModelsStatsActionResponseTests
        var deployment1 = new GetTrainedModelsStatsAction.Response.TrainedModelStats(
            "id1",
            new TrainedModelSizeStats(100, 200),
            GetTrainedModelsStatsActionResponseTests.randomIngestStats(),
            2,
            null,
            null
        );

        var deployment2 = new GetTrainedModelsStatsAction.Response.TrainedModelStats(
            "id1",
            new TrainedModelSizeStats(1, 2),
            GetTrainedModelsStatsActionResponseTests.randomIngestStats(),
            2,
            null,
            null
        );

        var dataframeConfig = DataFrameAnalyticsConfigTests.createRandom("dataframe1");
        var configs = List.of(
            TrainedModelConfigTests.createTestInstance(deployment1.getModelId()).setTags(List.of(dataframeConfig.getId())).build()
        );

        var table = action.buildTable(new FakeRestRequest(), List.of(deployment1, deployment2), configs, List.of(dataframeConfig));
        assertThat(table.getRows().get(0).get(0).value, is(deployment1.getModelId()));
        // pipeline count
        assertThat(table.getRows().get(0).get(9).value, is(4));
        // ingest count
        assertThat(
            table.getRows().get(0).get(10).value,
            is(deployment1.getIngestStats().totalStats().ingestCount() + deployment2.getIngestStats().totalStats().ingestCount())
        );
        // ingest time in millis
        assertThat(
            table.getRows().get(0).get(11).value,
            is(
                deployment1.getIngestStats().totalStats().ingestTimeInMillis() + deployment2.getIngestStats()
                    .totalStats()
                    .ingestTimeInMillis()
            )
        );
        // ingest current
        assertThat(
            table.getRows().get(0).get(12).value,
            is(deployment1.getIngestStats().totalStats().ingestCurrent() + deployment2.getIngestStats().totalStats().ingestCurrent())
        );
        // ingest failed count
        assertThat(
            table.getRows().get(0).get(13).value,
            is(
                deployment1.getIngestStats().totalStats().ingestFailedCount() + deployment2.getIngestStats()
                    .totalStats()
                    .ingestFailedCount()
            )
        );
        assertThat(table.getRows().get(0).get(14).value, is(dataframeConfig.getId()));
        assertThat(table.getRows().get(0).get(15).value, is(dataframeConfig.getCreateTime()));
        assertThat(table.getRows().get(0).get(16).value, is(Strings.arrayToCommaDelimitedString(dataframeConfig.getSource().getIndex())));
        assertThat(
            table.getRows().get(0).get(17).value,
            dataframeConfig.getAnalysis() == null ? nullValue() : is(dataframeConfig.getAnalysis().getWriteableName())
        );
    }
}
