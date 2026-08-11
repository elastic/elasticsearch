/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataSource;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.SourceVariant;
import org.junit.AfterClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The single, generic entry point for the whole public-data suite (elastic/esql-planning#1650): every
 * source/variant/spec in {@code public-data-catalog.yml} is exercised through this one class, cross-
 * producted by {@link #readScriptSpec}. There is deliberately no per-source subclass (unlike, say,
 * the local-fixture {@code ClickBenchParquetSpecIT}): a public-data source is pure catalog data, not code,
 * so onboarding one never needs a new Java class, only new catalog entries plus a csv-spec file (plan
 * section 4).
 * <p>
 * Selected via:
 * <ul>
 *   <li>{@code -Dtests.public_data.source=<id>[,<id>...]} -- one or more {@link PublicDataSource#id()}s;
 *       unset or blank runs every source in the catalog</li>
 *   <li>{@code -Dtests.public_data.spec=<substring>} -- restricts to variants whose
 *       {@link SourceVariant#specResource()} contains this substring; unset or blank runs every spec</li>
 *   <li>{@value PublicDataSpecTestCase#VARIANT_FILTER_PROPERTY} -- see
 *       {@link PublicDataSpecTestCase#readPublicDataSpec}</li>
 * </ul>
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class PublicDataSourcesIT extends PublicDataSpecTestCase {

    /** System property (Gradle-forwarded) selecting one or more comma-separated source ids; blank runs every source. */
    public static final String SOURCE_FILTER_PROPERTY = "tests.public_data.source";
    /** System property (Gradle-forwarded) restricting to spec resources containing this substring; blank runs every spec. */
    public static final String SPEC_FILTER_PROPERTY = "tests.public_data.spec";

    @ClassRule
    public static ElasticsearchCluster cluster = PublicDataClusters.build();

    public PublicDataSourcesIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        SourceVariant variant,
        PublicDataSource source
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, variant, source);
    }

    // Deliberately named (and signature-matched) identically to EsqlSpecTestCase#readScriptSpec(): that
    // method is itself @ParametersFactory-annotated with a *different* name (readScriptSpec), and
    // randomizedtesting's reflection-based factory discovery does not simply prefer the most-derived
    // declaration -- it unions the results of every distinctly-named @ParametersFactory method visible
    // across the whole class hierarchy. Naming this one identically makes it a plain Java static-method
    // hide instead, so only this override's ~20 catalog-driven tests run, never the entire default ESQL
    // csv-spec corpus that the parent's classpathResources("/*.csv-spec") glob would otherwise add.
    @ParametersFactory(argumentFormatting = "%1$s.%3$s[%8$s/%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        String sourceFilter = System.getProperty(SOURCE_FILTER_PROPERTY, "").trim();
        String specFilter = System.getProperty(SPEC_FILTER_PROPERTY, "").trim();

        List<String> sourceIds = new ArrayList<>();
        if (sourceFilter.isEmpty()) {
            for (PublicDataSource source : catalog().sources()) {
                sourceIds.add(source.id());
            }
        } else {
            sourceIds.addAll(Set.of(sourceFilter.split(",")));
        }

        List<Object[]> parameterized = new ArrayList<>();
        for (String sourceId : sourceIds) {
            for (Object[] test : readPublicDataSpec(sourceId.trim())) {
                SourceVariant variant = (SourceVariant) test[test.length - 2];
                if (specFilter.isEmpty() || variant.specResource().contains(specFilter)) {
                    parameterized.add(test);
                }
            }
        }
        return parameterized;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /** Cleans up every {@code data_source}/{@code dataset} this run registered, regardless of pass/fail. */
    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(adminClient());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    /** Releases {@link PinValidator}'s shared HTTP client threads so the SUITE-scope leak check passes. */
    @AfterClass
    public static void shutdownPinValidator() throws InterruptedException {
        PinValidator.shutdown();
    }
}
