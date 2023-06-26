/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class TransformIndexerStatsTests extends AbstractXContentSerializingTestCase<TransformIndexerStats> {

    public static TransformIndexerStats randomStats() {
        return new TransformIndexerStats(
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomDouble(),
            randomDouble(),
            randomDouble()
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected TransformIndexerStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected TransformIndexerStats mutateInstance(TransformIndexerStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<TransformIndexerStats> instanceReader() {
        return TransformIndexerStats::new;
    }

    @Override
    protected TransformIndexerStats doParseInstance(XContentParser parser) {
        return TransformIndexerStats.fromXContent(parser);
    }

    public void testExpAvgIncrement() {
        TransformIndexerStats stats = new TransformIndexerStats();

        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(0.0));

        stats.incrementCheckpointExponentialAverages(100, 20, 50);

        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(100.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(20.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(50.0));

        stats.incrementCheckpointExponentialAverages(150, 23, 100);

        assertThat(stats.getExpAvgCheckpointDurationMs(), closeTo(109.090909, 0.0000001));
        assertThat(stats.getExpAvgDocumentsIndexed(), closeTo(20.54545454, 0.0000001));
        assertThat(stats.getExpAvgDocumentsProcessed(), closeTo(59.0909090, 0.0000001));
    }

    public void testXContentLeniencyForMissingFields() throws IOException {
        // this is essentially the same test as done in the super class, but with the difference of a custom toXContent method that leaves
        // out fields if the value is 0, this allow us to test successful parsing if fields are not available, e.g. on older versions
        xContentTester(this::createParser, this::createTestInstance, TransformIndexerStatsTests::toXContentIfNotZero, this::doParseInstance)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(supportsUnknownFields())
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(assertToXContentEquivalence())
            .test();
    }

    public static void toXContentIfNotZero(TransformIndexerStats stats, XContentBuilder builder) throws IOException {
        // a toXContent version which leaves out field with value 0
        builder.startObject();
        xContentFieldIfNotZero(builder, TransformIndexerStats.NUM_PAGES.getPreferredName(), stats.getNumPages());
        xContentFieldIfNotZero(builder, TransformIndexerStats.NUM_INPUT_DOCUMENTS.getPreferredName(), stats.getNumDocuments());
        xContentFieldIfNotZero(builder, TransformIndexerStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(), stats.getOutputDocuments());
        xContentFieldIfNotZero(builder, TransformIndexerStats.NUM_DELETED_DOCUMENTS.getPreferredName(), stats.getNumDeletedDocuments());
        xContentFieldIfNotZero(builder, TransformIndexerStats.NUM_INVOCATIONS.getPreferredName(), stats.getNumInvocations());
        xContentFieldIfNotZero(builder, TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName(), stats.getIndexTime());
        xContentFieldIfNotZero(builder, TransformIndexerStats.INDEX_TOTAL.getPreferredName(), stats.getIndexTotal());
        xContentFieldIfNotZero(builder, TransformIndexerStats.INDEX_FAILURES.getPreferredName(), stats.getIndexFailures());
        xContentFieldIfNotZero(builder, TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName(), stats.getSearchTime());
        xContentFieldIfNotZero(builder, TransformIndexerStats.SEARCH_TOTAL.getPreferredName(), stats.getSearchTotal());
        xContentFieldIfNotZero(builder, TransformIndexerStats.PROCESSING_TIME_IN_MS.getPreferredName(), stats.getProcessingTime());
        xContentFieldIfNotZero(builder, TransformIndexerStats.PROCESSING_TOTAL.getPreferredName(), stats.getProcessingTotal());
        xContentFieldIfNotZero(builder, TransformIndexerStats.SEARCH_FAILURES.getPreferredName(), stats.getSearchFailures());
        xContentFieldIfNotZero(builder, TransformIndexerStats.DELETE_TIME_IN_MS.getPreferredName(), stats.getDeleteTime());
        xContentFieldIfNotZero(
            builder,
            TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
            stats.getExpAvgCheckpointDurationMs()
        );
        xContentFieldIfNotZero(
            builder,
            TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(),
            stats.getExpAvgDocumentsIndexed()
        );
        xContentFieldIfNotZero(
            builder,
            TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(),
            stats.getExpAvgDocumentsProcessed()
        );
        builder.endObject();
    }

    private static XContentBuilder xContentFieldIfNotZero(XContentBuilder builder, String name, long value) throws IOException {
        if (value > 0) {
            builder.field(name, value);
        }

        return builder;
    }

    private static XContentBuilder xContentFieldIfNotZero(XContentBuilder builder, String name, double value) throws IOException {
        if (value > 0.0) {
            builder.field(name, value);
        }

        return builder;
    }
}
