/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.record.CsvSpecRecorder;
import org.elasticsearch.xpack.esql.qa.publicdata.record.RecordedFragment;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;

/**
 * Base runner for the public-data suite: cross-products every workload test with the catalog's
 * filtered variants and resolves the spec's {@code {{corpus}}} template to the variant's pinned
 * remote resource. The JUnit name encodes every dimension
 * ({@code test {public-data:public-x.qNN{corpus-provider-format-codec-layout}}}), so a failing leg
 * greps straight back to its catalog cell.
 *
 * <p><b>Serial by construction:</b> the {@code dataset:} name is fixed by the query text
 * ({@code FROM <name>}), so successive variants re-{@code PUT} the same dataset name against the
 * shared cluster. Parallel forks would race on it — {@code maxParallelForks = 1} is enforced in
 * the Gradle task.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
@TimeoutSuite(millis = 240 * TimeUnits.MINUTE) // the base's 10 minutes cannot fit remote scans over multi-GiB objects
public abstract class PublicDataSpecTestCase extends EsqlSpecTestCase {

    /** Base keeps its copies private; the suite needs them for recording and retries. */
    protected final String specFileName;
    protected final String specTestName;
    protected final VariantSpec variant;

    protected PublicDataSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        VariantSpec variant
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
        this.specFileName = fileName;
        this.specTestName = testName;
        this.variant = variant;
    }

    /**
     * Expands the 6-arg tuples of {@code corpus}'s workload with every filtered active variant
     * (the {@code ClickBenchParquetSpecIT} idiom), applying each variant's {@code querySubset}
     * trim and the {@code -Dtests.public_data.shape} filter.
     */
    protected static List<Object[]> readScriptSpecWithVariants(PublicDataCatalog catalog, CorpusSpec corpus) throws Exception {
        PublicDataFilters filters = PublicDataFilters.fromSystemProperties();
        List<VariantSpec> variants = filters.variants(corpus);
        if (variants.isEmpty()) {
            return List.of();
        }
        List<URL> urls = classpathResources("/" + corpus.workload());
        List<Object[]> baseTests = SpecReader.readScriptSpec(urls, CsvSpecReader::specParser);
        Map<String, String> shapeByTest = readShapes(corpus);
        List<Object[]> parameterized = new ArrayList<>();
        for (VariantSpec variantSpec : variants) {
            for (Object[] base : baseTests) {
                String testName = baseName((String) base[2]);
                if (variantSpec.querySubset().isEmpty() == false && variantSpec.querySubset().contains(testName) == false) {
                    continue;
                }
                if (filters.shape() != null && filters.shape().equalsIgnoreCase(shapeByTest.getOrDefault(testName, "")) == false) {
                    continue;
                }
                Object[] expanded = Arrays.copyOf(base, base.length + 1);
                expanded[base.length] = variantSpec;
                parameterized.add(expanded);
            }
        }
        return parameterized;
    }

    private static Map<String, String> readShapes(CorpusSpec corpus) {
        Map<String, String> shapes = new HashMap<>();
        for (WorkloadSpec.TestSpec test : WorkloadSpec.loadFromClasspath(corpus.workload()).tests()) {
            if (test.readShape() != null) {
                shapes.put(test.baseName(), test.readShape().toLowerCase(Locale.ROOT));
            }
        }
        return shapes;
    }

    private static String baseName(String testName) {
        return testName.endsWith("-Ignore") ? testName.substring(0, testName.length() - "-Ignore".length()) : testName;
    }

    @Override
    protected final void doTest() throws Throwable {
        // Register the variant's data_source and resolve {{corpus}} to its pinned remote resource,
        // then run the spec query with bounded retries around transient store failures.
        String dataSource = DatasetRegistry.ensureDataSource(
            client(),
            variant.datasetSourceName(),
            variant.provider().esType(),
            variant.dataSourceSettings()
        );
        for (CsvSpecReader.DatasetSource source : testCase.datasetSources) {
            String resource = resolveTemplate(source.resource());
            PublicDataDatasets.ensureDataset(
                client(),
                source.name(),
                dataSource,
                resource,
                mergeWithSettings(source.withJson()),
                variant.datasetMappings()
            );
        }
        PublicDataRetry.run(specTestName + "{" + variant.label() + "}", () -> doTest(testCase.query));
    }

    /**
     * Resolves a {@code dataset:} directive's resource template against the variant under test:
     * {@code {{corpus}}} binds the whole corpus, {@code {{corpus:<name>}}} one of its declared
     * {@code sub_resources} — the form a multi-source {@code FROM d1, ..., dN} test uses to point
     * each of its datasets at a different remote location.
     */
    private String resolveTemplate(String resource) {
        if (resource.equals("{{corpus}}")) {
            return variant.resource();
        }
        if (resource.startsWith("{{corpus:") && resource.endsWith("}}")) {
            return variant.subResource(resource.substring("{{corpus:".length(), resource.length() - 2).trim());
        }
        throw new IllegalArgumentException(
            "dataset directive must bind {{corpus}} or {{corpus:<name>}} (validator-enforced), got [" + resource + "] in " + specTestName
        );
    }

    /**
     * Merges the variant's {@code datasetSettings} over the spec's {@code WITH {...}} options.
     * The spec carries corpus-wide format options; the variant carries physical-shape ones (e.g.
     * {@code header_row: false} for a headerless CSV leg), which win on conflict.
     */
    private Map<String, Object> mergeWithSettings(String specWithJson) throws IOException {
        Map<String, Object> merged = new HashMap<>();
        if (specWithJson != null) {
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, specWithJson)) {
                merged.putAll(parser.mapOrdered());
            }
        }
        merged.putAll(variant.datasetSettings());
        return merged;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        // Deliberately narrower than the base: no inference/views/source-mapping machinery here.
        // Capability gating plus the spec-local -Ignore mute (with its mandatory // defect: or
        // // disabled: block) are the only legitimate skips; infra trouble must FAIL, not skip.
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
    }

    @Override
    protected void assertResults(
        CsvTestUtils.ExpectedResults expected,
        List<Map<String, String>> actualColumns,
        List<List<Object>> actualValues,
        Logger logger
    ) {
        PublicDataFilters filters = PublicDataFilters.fromSystemProperties();
        if (filters.record()) {
            // Mismatch diagnostic only: capture what ES|QL actually returned, then assert as usual.
            new CsvSpecRecorder(PathUtils.get(filters.outputDir(), "recorded")).record(
                specFileName,
                new RecordedFragment(specTestName, variant.label(), CsvSpecRecorder.renderTable(actualColumns, actualValues))
            );
        }
        super.assertResults(expected, actualColumns, actualValues, logger);
    }

    @Override
    protected List<String> indicesToLoad() {
        return List.of(); // no local fixtures; the corpus IS the remote object store
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    @Override
    protected void createInferenceEndpointsIfSupported() {
        // no inference machinery on this cluster
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            // datasets first (this suite's own registrations), then DatasetRegistry's data sources
            PublicDataDatasets.cleanup(adminClient());
            DatasetRegistry.cleanup(adminClient());
        } finally {
            PublicDataDatasets.clearCaches();
            DatasetRegistry.clearCaches();
        }
    }
}
