/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction.Response;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class GetTrainedModelsStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<Response.TrainedModelStats> trainedModelStats = Stream.generate(() -> randomAlphaOfLength(10))
            .limit(listSize).map(id ->
                new Response.TrainedModelStats(id,
                    randomBoolean() ? randomIngestStats() : null,
                    randomIntBetween(0, 10))
            )
            .collect(Collectors.toList());
        return new Response(new QueryPage<>(trainedModelStats, randomLongBetween(listSize, 1000), Response.RESULTS_FIELD));
    }

    private IngestStats randomIngestStats() {
        List<String> pipelineIds = Stream.generate(()-> randomAlphaOfLength(10))
            .limit(randomIntBetween(0, 10))
            .collect(Collectors.toList());
        return new IngestStats(
            new IngestStats.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            pipelineIds.stream().map(id -> new IngestStats.PipelineStat(id, randomStats())).collect(Collectors.toList()),
            pipelineIds.stream().collect(Collectors.toMap(Function.identity(), (v) -> randomProcessorStats())));
    }

    private IngestStats.Stats randomStats(){
        return new IngestStats.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    private List<IngestStats.ProcessorStat> randomProcessorStats() {
        return Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomIntBetween(0, 10))
            .map(name -> new IngestStats.ProcessorStat(name, "inference", randomStats()))
            .collect(Collectors.toList());
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
