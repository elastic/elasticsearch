/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.matrix.AbstractMatrixStatsTestCase;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsResults;
import org.elasticsearch.search.aggregations.matrix.stats.RunningStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.matrix.AbstractMatrixStatsTestCase.DOUBLE_FIELD_NAME;

public class InternalPCAAggregationTests extends InternalAggregationTestCase<InternalPCAStats> {
    private ArrayList<String> fieldNames;
    private boolean hasPCAStatsResults;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hasPCAStatsResults = frequently();
        fieldNames =  hasPCAStatsResults ? AbstractMatrixStatsTestCase.randomNumericFields(randomIntBetween(2, 7)) : new ArrayList<>(0);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(getDefaultNamedXContents());
        ContextParser<Object, Aggregation> parser = (p, c) -> ParsedPCAStats.fromXContent(p, (String) c);
        namedXContents.add(new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(PCAAggregationBuilder.NAME), parser));
        return namedXContents;
    }

    @Override
    protected InternalPCAStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                  Map<String, Object> metaData) {
        RunningStats runningStats = new RunningStats();
        double[] values = new double[fieldNames.size()];
        int numDocs = randomIntBetween(100, 1000);
        for (int i = 0; i < numDocs; ++i) {
            int v = 0;
            for (String fieldName : fieldNames) {
                Number value;
                if (fieldName.contains(DOUBLE_FIELD_NAME)) {
                    value = randomDoubleBetween(-1000, 1000, true);
                } else {
                    value = randomInt();
                }
                values[v++] = value.doubleValue();
            }
            runningStats.add(fieldNames.stream().toArray(n -> new String[n]), values);
        }

        PCAStatsResults matrixStatsResults = hasPCAStatsResults ? new PCAStatsResults(runningStats) : null;
        return new InternalPCAStats(name, 1L, runningStats, matrixStatsResults, Collections.emptyList(), metaData);
    }

    @Override
    protected Writeable.Reader<InternalPCAStats> instanceReader() {
        return InternalPCAStats::new;
    }

    @Override
    protected void assertReduced(InternalPCAStats reduced, List<InternalPCAStats> inputs) {
        // todo implement
    }

    @Override
    protected void assertFromXContent(InternalPCAStats expected, ParsedAggregation parsedAggregation) throws IOException {
        assertTrue(parsedAggregation instanceof ParsedPCAStats);
    }

    @Override
    protected InternalPCAStats mutateInstance(InternalPCAStats instance) {
        String name = instance.getName();
        long docCount = instance.getDocCount();
        RunningStats runningStats = instance.getStats();
        MatrixStatsResults matrixStatsResults = instance.getResults();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                ArrayList<String> fields = new ArrayList<>(fieldNames);
                double[] values = new double[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    values[i] = randomDouble() * 200;
                }
                runningStats = new RunningStats();
                runningStats.add(fields.stream().toArray(n -> new String[n]), values);
                break;
            case 2:
                if (matrixStatsResults == null) {
                    matrixStatsResults = new PCAStatsResults(runningStats);
                } else {
                    matrixStatsResults = null;
                }
                break;
            case 3:
            default:
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
        }
        return new InternalPCAStats(name, docCount, runningStats, matrixStatsResults, Collections.emptyList(), metaData);
    }
}
