/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.oteldata;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class OtlpEscfBatchBenchmarkTests extends ESTestCase {

    private final int rows;
    private final int attributes;

    public OtlpEscfBatchBenchmarkTests(int rows, int attributes) {
        this.rows = rows;
        this.attributes = attributes;
    }

    public void testBuildBatchProducesOneRowPerGroup() throws IOException {
        OtlpEscfBatchBenchmark.Corpus corpus = corpus();
        try (EscfBatch batch = OtlpEscfBatchBenchmark.build(corpus.request, corpus.mappingHints, corpus.bytesRefRecycler)) {
            assertThat(batch.docCount(), equalTo(rows));
        } finally {
            corpus.tearDown();
        }
    }

    public void testScatterKeepsEveryRow() throws IOException {
        OtlpEscfBatchBenchmark.Corpus corpus = corpus();
        try {
            for (String partitions : Utils.possibleValues(OtlpEscfBatchBenchmark.Partitioning.class, "partitions")) {
                EscfBatch[] parts = OtlpEscfBatchBenchmark.scatterParts(corpus, partitioning(corpus, Integer.parseInt(partitions)));
                try {
                    int scatteredRows = 0;
                    for (EscfBatch part : parts) {
                        scatteredRows += part == null ? 0 : part.docCount();
                    }
                    assertThat("partitions " + partitions, scatteredRows, equalTo(rows));
                } finally {
                    Releasables.close(parts);
                }
            }
        } finally {
            corpus.tearDown();
        }
    }

    public void testSingleScatterMatchesSource() throws IOException {
        OtlpEscfBatchBenchmark.Corpus corpus = corpus();
        try {
            EscfBatch[] parts = OtlpEscfBatchBenchmark.scatterParts(corpus, partitioning(corpus, 1));
            try (EscfBatch part = parts[0]; EscfBatch parsed = EscfBatch.parse(part.data(), () -> {})) {
                for (int row = 0; row < rows; row++) {
                    String expected = render(corpus.batch, row);
                    assertThat("row " + row, render(part, row), equalTo(expected));
                    assertThat("serialized row " + row, render(parsed, row), equalTo(expected));
                }
            }
        } finally {
            corpus.tearDown();
        }
    }

    private OtlpEscfBatchBenchmark.Corpus corpus() throws IOException {
        OtlpEscfBatchBenchmark.Corpus corpus = new OtlpEscfBatchBenchmark.Corpus();
        corpus.rows = rows;
        corpus.attributes = attributes;
        corpus.setup(random());
        return corpus;
    }

    private OtlpEscfBatchBenchmark.Partitioning partitioning(OtlpEscfBatchBenchmark.Corpus corpus, int partitions) {
        OtlpEscfBatchBenchmark.Partitioning partitioning = new OtlpEscfBatchBenchmark.Partitioning();
        partitioning.partitions = partitions;
        partitioning.setup(corpus, random());
        return partitioning;
    }

    private static String render(SourceBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (String rows : Utils.possibleValues(OtlpEscfBatchBenchmark.Corpus.class, "rows")) {
            for (String attributes : Utils.possibleValues(OtlpEscfBatchBenchmark.Corpus.class, "attributes")) {
                parameters.add(new Object[] { Integer.parseInt(rows), Integer.parseInt(attributes) });
            }
        }
        return parameters;
    }
}
