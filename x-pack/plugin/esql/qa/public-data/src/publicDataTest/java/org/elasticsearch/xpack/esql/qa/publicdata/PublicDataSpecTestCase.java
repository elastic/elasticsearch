/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.Build;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.DatasetSource;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCodec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataSource;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.SourceVariant;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.RequestObjectBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResource;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for the public-data ES|QL suite (elastic/esql-planning#1650): cross-products every csv-spec
 * test under a {@link PublicDataSource} with each of its declared {@link SourceVariant}s, and runs the
 * spec's {@code FROM <name>} query against the variant's real, pinned, public S3/HTTPS object -- never a
 * local fixture. See the plan's section 4 for the full design.
 * <p>
 * Deliberately extends {@link EsqlSpecTestCase} directly rather than
 * {@code AbstractExternalSourceSpecTestCase}: that base class cross-products every spec with in-process
 * S3/GCS/Azure/HTTP/local fixtures, which conflicts with this suite's remote-only, no-fixture policy.
 */
public abstract class PublicDataSpecTestCase extends EsqlSpecTestCase {

    private static final Logger logger = LogManager.getLogger(PublicDataSpecTestCase.class);

    /** System property (Gradle-forwarded) selecting one variant id to run; blank runs every variant. */
    public static final String VARIANT_FILTER_PROPERTY = "tests.public_data.variant";
    /** System property (Gradle-forwarded) opting into {@link ResultRecorder} instead of asserting. */
    public static final String RECORD_PROPERTY = "tests.public_data.record";

    private static PublicDataCatalog catalog;

    protected static synchronized PublicDataCatalog catalog() {
        if (catalog == null) {
            catalog = PublicDataCatalog.loadFromClasspath();
        }
        return catalog;
    }

    /**
     * Cross-products every {@link SourceVariant} of {@code sourceId} (or the single variant selected by
     * {@value #VARIANT_FILTER_PROPERTY}, when set) with the tests declared in that variant's own
     * {@link SourceVariant#specResource()}, appending {@code (variant, source)} to each base parameter
     * tuple. A source spanning more than one {@code specResource} (see that field's Javadoc) only crosses
     * each variant with the spec it actually declares, never every spec with every variant, since two
     * variants of one source do not always expose the same rows. Intended for a subclass's
     * {@code @ParametersFactory} method.
     */
    protected static List<Object[]> readPublicDataSpec(String sourceId) throws Exception {
        PublicDataSource source = catalog().requireSourceId(sourceId);

        String variantFilter = System.getProperty(VARIANT_FILTER_PROPERTY, "").trim();
        List<SourceVariant> variants = new ArrayList<>();
        for (SourceVariant variant : source.variants()) {
            if (variantFilter.isEmpty() || variantFilter.equals(variant.id())) {
                variants.add(variant);
            }
        }
        if (variants.isEmpty()) {
            throw new IllegalStateException(
                "No variant of source [" + sourceId + "] matches " + VARIANT_FILTER_PROPERTY + "=[" + variantFilter + "]"
            );
        }

        // Group variants by the spec they answer, so each spec is parsed once and crossed only with the
        // variants that declare it.
        Map<String, List<SourceVariant>> variantsBySpec = new LinkedHashMap<>();
        for (SourceVariant variant : variants) {
            variantsBySpec.computeIfAbsent(variant.specResource(), key -> new ArrayList<>()).add(variant);
        }

        List<Object[]> parameterized = new ArrayList<>();
        for (Map.Entry<String, List<SourceVariant>> entry : variantsBySpec.entrySet()) {
            var url = classpathResource(entry.getKey());
            List<Object[]> baseTests = SpecReader.readScriptSpec(List.of(url), CsvSpecReader::specParser);
            for (Object[] baseTest : baseTests) {
                for (SourceVariant variant : entry.getValue()) {
                    Object[] extended = new Object[baseTest.length + 2];
                    System.arraycopy(baseTest, 0, extended, 0, baseTest.length);
                    extended[baseTest.length] = variant;
                    extended[baseTest.length + 1] = source;
                    parameterized.add(extended);
                }
            }
        }
        return parameterized;
    }

    protected final SourceVariant variant;
    protected final PublicDataSource source;
    /**
     * {@code EsqlSpecTestCase} keeps its own copy of the constructor's {@code testName} private, so this
     * suite keeps a copy too, for {@link #recordActual()}'s output filename.
     */
    private final String testName;

    protected PublicDataSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        SourceVariant variant,
        PublicDataSource source
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
        this.testName = testName;
        this.variant = variant;
        this.source = source;
    }

    /** No local index needs loading; every query in this suite reads only external datasets. */
    @Override
    protected List<String> indicesToLoad() {
        return List.of();
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected void createInferenceEndpointsIfSupported() throws IOException {
        // No spec in this suite exercises RERANK/COMPLETION/embeddings; skip provisioning entirely.
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    /**
     * Every dataset here is remote, multi-shard, and often multi-file (plan section 2's partitioning
     * dimension), so floating-point SUM/AVG accumulation order is not guaranteed to match whatever
     * single-pass order DuckDB/ClickHouse used when the checked-in expected value was established.
     * Rounding to 7 significant digits (the same tolerance the rest of the ESQL csv-spec corpus opts
     * into via this same hook) absorbs that last-few-ULPs drift without weakening the cross-validation
     * itself, since the row counts/columns being verified are exact either way.
     */
    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        assumeTrue(
            "FROM <dataset> requires the [dataset_in_from_command] capability",
            hasCapabilities(client(), List.of(EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.capabilityName()))
        );
        // Mirrors ExternalFileBzip2NdJsonCountIT: bzip2 is outside the GA text-format codec set and is
        // rejected on release builds (elastic/esql-planning#938). This suite's test JVM and the cluster it
        // stands up are built from the same Gradle run, so Build.current() here reflects the server's build
        // type too.
        if (variant.codec() == PublicDataCodec.BZIP2) {
            assumeTrue("bzip2 text-format codec is rejected on release builds", Build.current().isSnapshot());
        }
    }

    /**
     * Metadata-only pin re-check, then registers the {@code data_source}/{@code dataset}s this variant's
     * declared {@link DatasetSource}s need, then either records the actual answer
     * ({@value #RECORD_PROPERTY}{@code =true}) or asserts it against the checked-in expected results
     * (the normal path, inherited from {@link EsqlSpecTestCase#doTest(String)}).
     */
    @Override
    protected void doTest() throws Throwable {
        try {
            PinValidator.verify(variant);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while pin-checking variant [" + variant.id() + "]", e);
        }

        String dataSourceName = ensureDataSourceForVariant();
        for (DatasetSource declared : testCase.datasetSources) {
            String resource = resolveTemplate(declared.resource());
            DatasetRegistry.ensureDataset(client(), declared.name(), dataSourceName, resource, variant.settingsJson());
        }

        if (isRecordMode()) {
            recordActual();
            return;
        }
        doTest(testCase.query);
    }

    /** Whether {@code {{<source.id()>}}} in a dataset directive's resource is present and needs substitution. */
    private String resolveTemplate(String resource) {
        String template = "{{" + source.id() + "}}";
        if (resource.equals(template) == false) {
            throw new IllegalArgumentException(
                "Public-data spec ["
                    + variant.specResource()
                    + "] dataset resource ["
                    + resource
                    + "] must be exactly ["
                    + template
                    + "]; every variant substitution happens through the catalog, never a partial template"
            );
        }
        return variant.resource();
    }

    /**
     * Registers (once per variant's provider+region) a {@code data_source} authenticating anonymously,
     * matching every already-public source in the catalog. Named after the provider/region rather than
     * the source/variant so that two variants sharing a provider+region reuse one {@code data_source}.
     */
    private String ensureDataSourceForVariant() throws IOException {
        String region = variant.region();
        String name = "public_data_"
            + variant.provider().name().toLowerCase(Locale.ROOT)
            + (region == null ? "" : "_" + region.replace('-', '_'));
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("auth", "anonymous");
        if (region != null) {
            settings.put("region", region);
        }
        return DatasetRegistry.ensureDataSource(client(), name, variant.provider().dataSourceType(), settings);
    }

    /**
     * Runs {@code testCase.query} without asserting, and hands the raw response to {@link ResultRecorder}.
     * See the class Javadoc on {@link ResultRecorder} for why this is never treated as ground truth.
     */
    @SuppressWarnings("unchecked")
    private void recordActual() throws IOException {
        RequestObjectBuilder builder = new RequestObjectBuilder(XContentType.JSON).query(testCase.query);
        Map<String, Object> answer = RestEsqlTestCase.runEsql(builder, testCase.assertWarnings(deduplicateExactWarnings()), null, mode);
        List<Map<String, String>> columns = (List<Map<String, String>>) answer.get("columns");
        List<List<Object>> values = (List<List<Object>>) answer.get("values");
        Path buildDir = Path.of(System.getProperty("tests.public_data.build_dir", "build"));
        ResultRecorder.record(buildDir, source.id(), variant, sanitizedTestName(), columns, values);
    }

    private static boolean isRecordMode() {
        return Boolean.getBoolean(RECORD_PROPERTY);
    }

    /** A filesystem-safe test name for {@link ResultRecorder}'s output filename. */
    private String sanitizedTestName() {
        return testName.replaceAll("[^a-zA-Z0-9_.-]+", "_");
    }
}
