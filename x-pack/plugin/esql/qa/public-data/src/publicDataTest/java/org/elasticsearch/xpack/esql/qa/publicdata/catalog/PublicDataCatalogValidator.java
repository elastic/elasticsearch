/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.DatasetSource;
import org.elasticsearch.xpack.esql.SpecReader;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Offline (no-network, no-cluster) checks over a {@link PublicDataCatalog} and its referenced csv-spec
 * files, run by the {@code catalogValidation} Gradle task (plan section 5). Every violation is collected
 * and returned rather than thrown on the first one, so a single run surfaces every problem instead of
 * requiring one fix-and-rerun cycle per issue.
 */
public final class PublicDataCatalogValidator {

    /** Default cap on a single query's result-row count; {@link #ABSOLUTE_MAX_RESULT_ROWS} may never be exceeded. */
    public static final int DEFAULT_MAX_RESULT_ROWS = 300;
    /** Absolute cap on a single query's result-row count (plan section 5); never raised per-query. */
    public static final int ABSOLUTE_MAX_RESULT_ROWS = 1000;

    private PublicDataCatalogValidator() {}

    /** Validates the whole catalog: every source's variants and its checked-in csv-spec file. */
    public static List<String> validate(PublicDataCatalog catalog) {
        List<String> problems = new ArrayList<>();
        Set<String> allVariantResources = new HashSet<>();
        for (PublicDataSource source : catalog.sources()) {
            validateSource(source, problems);
            for (SourceVariant variant : source.variants()) {
                if (allVariantResources.add(variant.provider() + "|" + variant.resource()) == false) {
                    problems.add("Duplicate resource across variants: [" + variant.resource() + "]");
                }
            }
            validateSpecs(source, problems);
        }
        return problems;
    }

    private static void validateSource(PublicDataSource source, List<String> problems) {
        for (SourceVariant variant : source.variants()) {
            String ctx = source.id() + "." + variant.id();
            if (variant.resource().contains("file://")) {
                problems.add("[" + ctx + "] resource must never be a file:// URI: [" + variant.resource() + "]");
            }
            if (isTextFormat(variant.format()) && variant.codec() == PublicDataCodec.UNCOMPRESSED) {
                problems.add(
                    "["
                        + ctx
                        + "] pairs a text format ("
                        + variant.format()
                        + ") with UNCOMPRESSED; text variants in this catalog are only ever exercised "
                        + "whole-object-compressed with ZSTD or GZIP"
                );
            }
            if (variant.codec() == PublicDataCodec.SNAPPY && isTextFormat(variant.format())) {
                problems.add(
                    "["
                        + ctx
                        + "] pairs a text format ("
                        + variant.format()
                        + ") with SNAPPY, which this catalog reserves for Parquet page compression"
                );
            }
            if (variant.pin().sizeBytes() <= 0) {
                problems.add("[" + ctx + "] pin.size_bytes must be positive, got [" + variant.pin().sizeBytes() + "]");
            }
            if (variant.pin().strategy() == PinStrategy.CONTENT_SIGNATURE
                && (variant.pin().contentSignature() == null || variant.pin().contentSignature().isBlank())) {
                // PublicDataCatalog.parsePin already enforces this at parse time; re-checked here so a
                // future direct-construction path (e.g. a test helper) can't silently skip it either.
                problems.add("[" + ctx + "] pin.strategy CONTENT_SIGNATURE requires a non-blank pin.content_signature");
            }
            if ((variant.partitionLayout() == PartitionLayout.HIVE_PARTITIONED
                || variant.partitionLayout() == PartitionLayout.NESTED_HIVE_PARTITIONED
                || variant.partitionLayout() == PartitionLayout.MANY_SMALL_FILES
                || variant.partitionLayout() == PartitionLayout.UNIFORM_SHARDS
                || variant.partitionLayout() == PartitionLayout.SKEWED_SHARDS) && variant.pin().objectCount() == null) {
                problems.add("[" + ctx + "] partition_layout [" + variant.partitionLayout() + "] requires pin.object_count");
            }
            if (variant.partitionLayout() == PartitionLayout.SINGLE_FILE && variant.resource().contains("*")) {
                problems.add("[" + ctx + "] partition_layout SINGLE_FILE but resource contains a glob: [" + variant.resource() + "]");
            }
        }
    }

    private static boolean isTextFormat(PublicDataFormat format) {
        return format == PublicDataFormat.NDJSON || format == PublicDataFormat.CSV || format == PublicDataFormat.TSV;
    }

    /**
     * Parses every distinct {@link SourceVariant#specResource()} this source's variants declare (there
     * may be more than one -- see that field's Javadoc) with the real {@code CsvSpecReader} (the same
     * parser the runner uses) and checks: every declared test has exactly one {@link DatasetSource}, whose
     * resource is exactly {@code {{<source.id()>}}} (never a partial template or a literal path -- see
     * {@code PublicDataSpecTestCase#resolveTemplate}); every test name is unique within its spec file (the
     * reader itself already enforces this, but a duplicate throws there rather than reporting cleanly, so
     * no separate check is needed here); and no result set exceeds {@link #ABSOLUTE_MAX_RESULT_ROWS} rows.
     */
    private static void validateSpecs(PublicDataSource source, List<String> problems) {
        Set<String> specResources = new LinkedHashSet<>();
        for (SourceVariant variant : source.variants()) {
            specResources.add(variant.specResource());
        }
        for (String specResource : specResources) {
            validateSpec(source, specResource, problems);
        }
    }

    private static void validateSpec(PublicDataSource source, String specResource, List<String> problems) {
        URL specUrl = PublicDataCatalogValidator.class.getResource(specResource);
        if (specUrl == null) {
            problems.add("[" + source.id() + "] spec resource not found on classpath: [" + specResource + "]");
            return;
        }
        String expectedTemplate = "{{" + source.id() + "}}";
        List<Object[]> tests;
        try {
            tests = SpecReader.readScriptSpec(List.of(specUrl), CsvSpecReader::specParser);
        } catch (Exception e) {
            problems.add("[" + source.id() + "] failed to parse spec [" + specResource + "]: " + e.getMessage());
            return;
        }
        Set<String> seenTestNames = new HashSet<>();
        for (Object[] test : tests) {
            String testName = (String) test[2];
            CsvTestCase testCase = (CsvTestCase) test[4];
            String testCtx = source.id() + "." + testName;

            if (seenTestNames.add(testName) == false) {
                problems.add("[" + testCtx + "] duplicate test name within [" + specResource + "]");
            }
            if (testCase.datasetSources.size() != 1) {
                problems.add(
                    "["
                        + testCtx
                        + "] declares ["
                        + testCase.datasetSources.size()
                        + "] dataset: directives; every public-data test must declare exactly one"
                );
            } else {
                DatasetSource declared = testCase.datasetSources.get(0);
                if (declared.resource().equals(expectedTemplate) == false) {
                    problems.add(
                        "["
                            + testCtx
                            + "] dataset directive resource ["
                            + declared.resource()
                            + "] must be exactly ["
                            + expectedTemplate
                            + "]"
                    );
                }
            }
            int rows = countResultRows(testCase.expectedResults);
            if (rows > ABSOLUTE_MAX_RESULT_ROWS) {
                problems.add(
                    "["
                        + testCtx
                        + "] result set has ["
                        + rows
                        + "] rows, exceeding the absolute maximum of ["
                        + ABSOLUTE_MAX_RESULT_ROWS
                        + "]"
                );
            }
        }
    }

    /** {@code expectedResults} is the header line followed by every data row, each newline-terminated. */
    private static int countResultRows(String expectedResults) {
        if (expectedResults == null || expectedResults.isBlank()) {
            return 0;
        }
        String[] lines = expectedResults.split("\r?\n");
        int rows = 0;
        for (String line : lines) {
            if (line.isBlank() == false) {
                rows++;
            }
        }
        // The first non-blank line is the `name:type | ...` header, not a data row.
        return Math.max(0, rows - 1);
    }
}
