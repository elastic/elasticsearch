/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Runs the same Parquet format spec tests as ParquetFormatSpecIT but using the parquet-rs native reader.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetRsFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public ParquetRsFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
    }

    @Override
    protected String readerName() {
        return FormatNameResolver.READER_PARQUET_RS;
    }

    /**
     * The parquet-rs native reader registers no file extension and the dataset model has no reader/format
     * selector, so parquet-rs is unreachable via {@code FROM <dataset>}. This suite is therefore a sanctioned
     * EXTERNAL holdout (like gRPC/Flight and Iceberg): it rebuilds each {@code FROM <dataset>} spec into an
     * {@code EXTERNAL ... WITH "reader": "parquet-rs"} query. See {@link #forceExternalRebuild()}.
     */
    @Override
    protected boolean forceExternalRebuild() {
        return true;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    private static final Set<String> SKIPPED_TESTS = Set.of(
        // unknown parquet column [job_positions] referenced in projection (reported in the schema as "element")
        "filterFirstRowAllColumns",
        "mvAppendFromScalars",
        "mvConcatFromSplit",
        "mvCountFromSplit",
        "mvDedupeFromSplit",
        "mvExpandFromSplit",
        "mvMaxFromSplit",
        "mvMinFromSplit",
        // unknown parquet column [salary_change] referenced in projection (reported in the schema as "element")
        "mvDedupeFromSplit2",
        // unknown parquet column [author] referenced in projection
        "externalRerankBooks",
        // TODO: parquet-rs OrdinalBytesRefBlock validity buffer is not 8-byte padded per the Arrow
        // columnar spec; AbstractArrowBufBlock.areAllValuesNull reads it as a long and throws
        // IndexOutOfBoundsException. Both the multi-key PackedValuesBlockHash path and the single-key
        // BytesRefBlockHash path (line 77) call areAllValuesNull, so any STATS … BY <keyword> on a
        // multi-file glob fails with "index: 0, length: 8 (expected: range(0, N))". Single-file STATS BY
        // tests pass only because the standalone parquet-rs output happens to allocate a buffer ≥ 8 bytes.
        // Re-enable once the parquet-rs reader pads validity buffers correctly.
        "hivePartitionStatsByLangAndGender",
        "aggregateMultiFileByGender",
        "multiFileEvalAndAggregate",
        "multiFileGroupByFile",
        "ffwAggregateByGender",
        "strictAggregateByGender",
        "ubnAggregateByGender",
        // Nested STRUCT subfield projection (external-nested-struct.csv-spec) is implemented by the
        // Java parquet reader only; parquet-rs does not yet flatten struct schemas.
        "nestedKeepSingleSubfield",
        "nestedKeepTwoSubfieldsSameParent",
        "nestedKeepMixedTopLevelAndNested",
        "nestedStatsByNested",
        "nestedNullPropagation",
        "nestedWhereEquals",
        "nestedWhereIsNull",
        "nestedStatsMinMax",
        "nestedFilterAndProjectMixed",
        // A LIST leaf reached through a STRUCT (e.g. answers.text where answers is struct<text: list<...>>)
        // is bound by its flattened logical name only by the Java parquet reader; parquet-rs does not yet
        // flatten struct schemas, so the leaf resolves to no column descriptor and reads as all-null
        // (COUNT returns 0, values/min/max are null). Re-enable once parquet-rs binds list-under-struct
        // leaves by their flattened name in both the read and aggregate-statistics paths.
        "listUnderStructCount",
        "listUnderStructValues",
        "listUnderStructMvCount",
        "listUnderStructIsNull",
        "listUnderStructMinMax"
    );

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " not supported by parquet-rs reader", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-*.csv-spec");
    }
}
