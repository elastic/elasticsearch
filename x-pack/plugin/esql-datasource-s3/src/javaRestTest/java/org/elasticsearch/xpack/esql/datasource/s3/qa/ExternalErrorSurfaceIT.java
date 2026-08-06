/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.AwsCredentialsUtils;
import fixture.aws.DynamicRegionSupplier;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Sweeps the whole user-visible error surface of external datasets — every misconfiguration we can
 * construct against a real S3 endpoint, plus the dataset/data-source CRUD APIs — and records the
 * status code, exception type, top-level {@code reason} and the full {@code caused_by} chain that
 * each one produces.
 *
 * <p>This is a <em>discovery</em> harness first and a regression pin second. The recorded matrix is
 * written to {@code build/external-error-surface.md} (overridable with {@code -Dtests.error.report})
 * so the messages can be read side by side; the assertions at the end are deliberately about
 * properties that hold across the whole matrix rather than about individual strings:
 * <ul>
 *   <li>the top-level {@code reason} must not collapse distinct root conditions onto one string, and</li>
 *   <li>the top-level {@code reason} must not contain a JVM type name, which is what a flattened cause
 *   chain looks like from outside.</li>
 * </ul>
 * <b>Status codes are deliberately not asserted.</b> The matrix shows several user-caused conditions
 * reported as 5xx, which is wrong, but correcting a status is a compatibility-visible change and is
 * being handled separately from the message work; the per-probe status is recorded in the report so the
 * follow-up has its evidence. Adding the assertion here is the first step of that follow-up.
 * Each violation is collected and reported together, so one run tells us everything that is wrong
 * rather than stopping at the first case.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ExternalErrorSurfaceIT extends ESRestTestCase {

    private static final String BUCKET = "error-surface-bucket";
    private static final String ACCESS_KEY = "error_surface_access_key";
    private static final String SECRET_KEY = "error_surface_secret_key";

    private static final DynamicRegionSupplier regionSupplier = new DynamicRegionSupplier();

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(
        BUCKET,
        AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    // Registering a data source with access_key/secret_key encrypts those secrets into cluster state,
    // which needs a project encryption key installed. Without it every PUT data_source returns 503.
    // Same shape as esql-datasource-csv's Clusters.testClusterWithEncryption.
    private static final String ENCRYPTION_PASSWORD_ID = "test";

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "esql-error-surface-encryption-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .environment("AWS_REGION", regionSupplier)
        // Keep the AWS SDK away from ~/.aws, which it is not entitled to read.
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    // ---------------------------------------------------------------------------------------------
    // Seed corpus. Every blob here is a control: the matrix below misconfigures access to these, so
    // a failure is always attributable to the configuration and never to a broken object.
    // ---------------------------------------------------------------------------------------------

    private static final String GOOD_CSV = "data/good.csv";
    private static final String GOOD_NDJSON = "data/good.ndjson";
    private static final String GOOD_TSV = "data/good.tsv";
    private static final String NO_EXTENSION = "data/good_no_extension";
    private static final String UNKNOWN_EXTENSION = "data/good.xyz";
    private static final String EMPTY_CSV = "data/empty.csv";
    private static final String EMPTY_TSV = "data/empty.tsv";
    private static final String GARBAGE_PARQUET = "data/garbage.parquet";
    private static final String TRUNCATED_PARQUET = "data/truncated.parquet";
    private static final String RAGGED_CSV = "data/ragged.csv";
    private static final String GLOB_A = "glob/a.csv";
    private static final String GLOB_B = "glob/b.csv";
    private static final String GLOB_INCOMPATIBLE = "glob/c.csv";

    @BeforeClass
    public static void seedFixture() {
        seed(GOOD_CSV, "id,city\n1,Vienna\n2,Berlin\n");
        seed(GOOD_NDJSON, "{\"id\":1,\"city\":\"Vienna\"}\n{\"id\":2,\"city\":\"Berlin\"}\n");
        seed(GOOD_TSV, "id\tcity\n1\tVienna\n2\tBerlin\n");
        seed(NO_EXTENSION, "id,city\n1,Vienna\n");
        seed(UNKNOWN_EXTENSION, "id,city\n1,Vienna\n");
        seed(EMPTY_CSV, "");
        seed(EMPTY_TSV, "");
        // Long enough that a footer read has bytes to work with, but structurally not Parquet.
        seed(GARBAGE_PARQUET, "this is plain text pretending to be a parquet file, repeated to give it some length.\n".repeat(8));
        // Correct leading magic, nothing else — the shape a half-uploaded file has.
        seed(TRUNCATED_PARQUET, "PAR1" + "x".repeat(64));
        // Row 2 has one field too many for the declared header.
        seed(RAGGED_CSV, "id,city\n1,Vienna\n2,Berlin,extra\n");
        seed(GLOB_A, "id,city\n1,Vienna\n");
        seed(GLOB_B, "id,city\n2,Berlin\n");
        // Same column count, different names — forces schema reconciliation across the glob.
        seed(GLOB_INCOMPATIBLE, "ident,town\n3,Prague\n");
    }

    private static void seed(String key, String content) {
        s3HttpFixture.seedBlob(key, content.getBytes(StandardCharsets.UTF_8));
    }

    private static String s3(String key) {
        return "s3://" + BUCKET + "/" + key;
    }

    // ---------------------------------------------------------------------------------------------
    // Recorded outcome of one probe.
    // ---------------------------------------------------------------------------------------------

    /**
     * @param group      the axis being probed, used to group the report
     * @param name       what was misconfigured
     * @param expectation what a user should be told; free text, read by a human against {@code reason}
     * @param status     HTTP status actually returned ({@code 200} means the request unexpectedly succeeded)
     * @param type       {@code error.type}
     * @param reason     {@code error.reason} — the string every client surfaces
     * @param causeChain flattened {@code caused_by} chain, outermost first
     */
    private record Probe(String group, String name, String expectation, int status, String type, String reason, List<String> causeChain) {}

    private final List<Probe> probes = new ArrayList<>();

    /**
     * Groups of probes that describe the <em>same</em> underlying condition reached through different APIs. Sharing
     * one message across these is correct — "data source [x] not found" should read identically whether you asked
     * for it, deleted it, or referenced it from a dataset — so they are exempt from the collision check. Everything
     * outside these groups is a genuinely distinct condition and must be distinguishable from its message alone.
     */
    private static final List<Set<String>> SHARED_CONDITIONS = List.of(
        Set.of("put dataset referencing an unknown data source", "get an unknown data source", "delete an unknown data source"),
        Set.of("get an unknown dataset", "delete an unknown dataset"),
        // Both are "the resource is not a supported object location"; the message echoes what was given.
        Set.of("unsupported URI scheme", "URI with no scheme at all"),
        // The reported_case probes repeat conditions covered elsewhere on a .tsv, deliberately: they exist to
        // keep the exact user-reported shape visible in the report, not to add a new condition.
        Set.of("tsv object does not exist", "object key does not exist"),
        Set.of("tsv object is empty", "zero-byte object"),
        Set.of("tsv declared as parquet", "explicit format contradicts the bytes (parquet declared, CSV content)"),
        // The store answers both with an identical 403 AccessDenied, so the message cannot tell them apart from
        // the response alone. Naming the configured auth mode would ("…AccessDenied, data source uses
        // auth=anonymous"), but that is local knowledge the storage object does not currently carry.
        Set.of("wrong access key", "anonymous access against an authenticated endpoint")
    );

    /**
     * Conditions whose message is knowingly still not distinguishing, with the reason. An entry here is a decision
     * on record, not a silenced failure — delete it when the underlying issue is fixed and the gate will hold the
     * new behaviour.
     */
    private static final Map<String, String> KNOWN_OPEN = Map.of(
        "key is a prefix, not an object",
        "reports \"Object not found\", the same as a genuinely absent key. The store can tell the two apart -- a "
            + "prefix has children a listing would return -- so this is a defect to improve, not one condition "
            + "wearing two names. Recorded here rather than in SHARED_CONDITIONS so the gate can hold an improvement",
        "no extension and no explicit format",
        "reports the Iceberg catalog's own failure rather than \"the format cannot be inferred; set [format]\". "
            + "IcebergTableCatalog#canHandle claims every s3:// path, so it claims an extensionless object, fails "
            + "for its own reasons, and its failure is the one surfaced -- the no-reader message never fires because "
            + "a factory did claim. Narrowing that claim is a behaviour change, not a message fix"
    );

    // ---------------------------------------------------------------------------------------------
    // The matrix.
    // ---------------------------------------------------------------------------------------------

    public void testExternalErrorSurfaceMatrix() throws Exception {
        // A working data source and dataset, so every negative case below differs from a known-good
        // baseline by exactly one thing.
        putDataSource("good_ds", staticCredentialSettings());
        putDataset("good_ds_rows", "good_ds", s3(GOOD_CSV), null, null);
        assertQuerySucceeds("FROM good_ds_rows | STATS c = COUNT(*)");

        sweepReportedCase();
        sweepObjectAddressing();
        sweepCredentials();
        sweepFormat();
        sweepReaderOptions();
        sweepDeclaredMapping();
        sweepGlobs();
        sweepDatasetCrud();
        sweepDataSourceCrud();
        sweepReferentialIntegrity();

        Path report = writeReport();
        logger.info("external error surface report written to {}", report.toAbsolutePath());

        assertMatrixInvariants();
    }

    // -------- the reported case ------------------------------------------------------------------

    /**
     * The shape users actually hit, kept as its own group because it is the one that gets reported:
     * a {@code .tsv} on S3 that will not resolve, which produced
     * <pre>
     * Failed to resolve external source [s3://&lt;bucket&gt;/titanic.tsv]:
     *   java.lang.IllegalArgumentException: Failed to resolve metadata for [s3://&lt;bucket&gt;/titanic.tsv]
     * </pre>
     * — a JVM type name in prose, a sentence that repeats the path and says nothing else, and a 500 for a
     * misconfiguration. Four different reasons a {@code .tsv} can fail to resolve are probed here; before the fix
     * they were indistinguishable from each other and from the CSV and Parquet failures elsewhere in this matrix,
     * which is why the message was useless to whoever received it.
     */
    private void sweepReportedCase() throws IOException {
        queryProbe(
            "reported_case",
            "tsv object does not exist",
            "say the object does not exist and name it",
            "titanic_missing",
            "good_ds",
            s3("titanic.tsv"),
            null
        );
        queryProbe(
            "reported_case",
            "tsv object is empty",
            "say the object has no header/schema line",
            "titanic_empty",
            "good_ds",
            s3(EMPTY_TSV),
            null
        );
        queryProbeWithSettings(
            "reported_case",
            "tsv declared as parquet",
            "say the object could not be read as the declared format",
            "titanic_wrong_format",
            "good_ds",
            s3(GOOD_TSV),
            Map.of("format", "parquet")
        );
        queryProbe(
            "reported_case",
            "tsv under a data source with the wrong credentials",
            "say access to the object store was refused",
            "titanic_bad_creds",
            "bad_key_ds",
            s3(GOOD_TSV),
            () -> putDataSource(
                "bad_key_ds",
                Map.of(
                    "access_key",
                    "not_the_right_key",
                    "secret_key",
                    SECRET_KEY,
                    "region",
                    regionSupplier.get(),
                    "endpoint",
                    s3HttpFixture.getAddress()
                )
            )
        );
    }

    // -------- object addressing ------------------------------------------------------------------

    private void sweepObjectAddressing() throws IOException {
        queryProbe(
            "addressing",
            "object key does not exist",
            "name the missing key and say it does not exist",
            "missing_key",
            "good_ds",
            s3("data/definitely-not-here.csv"),
            null
        );
        queryProbe(
            "addressing",
            "bucket does not exist",
            "name the bucket and say it does not exist (distinct from a missing key)",
            "missing_bucket",
            "good_ds",
            "s3://no-such-bucket-at-all/data/good.csv",
            null
        );
        queryProbe(
            "addressing",
            "key is a prefix, not an object",
            "say the path addresses no object; suggest a glob if a prefix was meant",
            "prefix_not_object",
            "good_ds",
            s3("data"),
            null
        );
        queryProbe(
            "addressing",
            "unsupported URI scheme",
            "name the scheme and list the supported ones",
            "bad_scheme",
            "good_ds",
            "ftp://" + BUCKET + "/data/good.csv",
            null
        );
        queryProbe(
            "addressing",
            "scheme with no host or key",
            "say the URI is not a complete object location",
            "empty_uri",
            "good_ds",
            "s3://",
            null
        );
        queryProbe(
            "addressing",
            "URI with no scheme at all",
            "say a scheme is required and list the supported ones",
            "no_scheme",
            "good_ds",
            "/data/good.csv",
            null
        );
        queryProbe(
            "addressing",
            "endpoint refuses connections",
            "say the endpoint is unreachable; must be retryable (5xx/503), not a client error",
            "dead_endpoint",
            "dead_endpoint_ds",
            s3(GOOD_CSV),
            () -> putDataSource(
                "dead_endpoint_ds",
                Map.of(
                    "access_key",
                    ACCESS_KEY,
                    "secret_key",
                    SECRET_KEY,
                    "region",
                    regionSupplier.get(),
                    // Reserved discard port: connect always fails fast.
                    "endpoint",
                    "http://127.0.0.1:9"
                )
            )
        );
    }

    // -------- credentials ------------------------------------------------------------------------

    private void sweepCredentials() throws IOException {
        queryProbe(
            "credentials",
            "wrong access key",
            "say authentication/authorization to the object store failed and which data source it was",
            "bad_key",
            "bad_key_ds",
            s3(GOOD_CSV),
            () -> putDataSource(
                "bad_key_ds",
                Map.of(
                    "access_key",
                    "not_the_right_key",
                    "secret_key",
                    SECRET_KEY,
                    "region",
                    regionSupplier.get(),
                    "endpoint",
                    s3HttpFixture.getAddress()
                )
            )
        );
        queryProbe(
            "credentials",
            "anonymous access against an authenticated endpoint",
            "say anonymous access was refused and name the setting that selects credentials",
            "anonymous",
            "anonymous_ds",
            s3(GOOD_CSV),
            () -> putDataSource(
                "anonymous_ds",
                Map.of("auth", "anonymous", "region", regionSupplier.get(), "endpoint", s3HttpFixture.getAddress())
            )
        );
    }

    // -------- format selection -------------------------------------------------------------------

    private void sweepFormat() throws IOException {
        queryProbe(
            "format",
            "no extension and no explicit format",
            "say the format could not be determined and name the [format] setting",
            "no_ext",
            "good_ds",
            s3(NO_EXTENSION),
            null
        );
        queryProbe(
            "format",
            "unknown extension and no explicit format",
            "name the unrecognized extension and list the known formats",
            "unknown_ext",
            "good_ds",
            s3(UNKNOWN_EXTENSION),
            null
        );
        queryProbeWithSettings(
            "format",
            "explicit format contradicts the bytes (parquet declared, CSV content)",
            "say the object could not be read as the declared format",
            "csv_as_parquet",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("format", "parquet")
        );
        queryProbeWithSettings(
            "format",
            "unknown explicit format name",
            "name the unknown format and list the known ones",
            "bogus_format",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("format", "not_a_format")
        );
        queryProbe(
            "format",
            "parquet extension over non-parquet bytes",
            "say the object is not a valid Parquet file",
            "garbage_parquet",
            "good_ds",
            s3(GARBAGE_PARQUET),
            null
        );
        queryProbe(
            "format",
            "parquet with correct magic but truncated body",
            "say the Parquet footer is unreadable/truncated",
            "truncated_parquet",
            "good_ds",
            s3(TRUNCATED_PARQUET),
            null
        );
        queryProbe("format", "zero-byte object", "say the object is empty", "empty_csv", "good_ds", s3(EMPTY_CSV), null);
    }

    // -------- reader options ---------------------------------------------------------------------

    private void sweepReaderOptions() throws IOException {
        queryProbeWithSettings(
            "reader_options",
            "unknown setting key on the dataset",
            "name the unknown key and list the accepted ones",
            "unknown_key",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("delimter", ",")
        );
        queryProbeWithSettings(
            "reader_options",
            "multi-character delimiter",
            "say the delimiter must be a single character",
            "multichar_delim",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("delimiter", "||")
        );
        queryProbeWithSettings(
            "reader_options",
            "invalid encoding name",
            "name the invalid encoding",
            "bad_encoding",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("encoding", "utf-99")
        );
        queryProbeWithSettings(
            "reader_options",
            "invalid datetime format pattern",
            "name the invalid pattern and where it came from",
            "bad_datetime_format",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("datetime_format", "not-a-pattern[[[")
        );
        queryProbeWithSettings(
            "reader_options",
            "non-boolean header_row",
            "say header_row must be a boolean and echo what was given",
            "bad_header_row",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("header_row", "maybe")
        );
        queryProbeWithSettings(
            "reader_options",
            "negative schema_sample_size",
            "say the bound and echo what was given",
            "negative_sample",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("schema_sample_size", "-5")
        );
        queryProbeWithSettings(
            "reader_options",
            "multi-character quote character",
            "say the quote must be a single character",
            "multichar_quote",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("quote", "''")
        );
        queryProbeWithSettings(
            "reader_options",
            "unknown error_mode value",
            "name the invalid value and list the accepted ones",
            "bad_error_mode",
            "good_ds",
            s3(GOOD_CSV),
            Map.of("error_mode", "fail")
        );
        queryProbeWithSettings(
            "reader_options",
            "row with more fields than the header",
            "name the row and the column count mismatch",
            "ragged_row",
            "good_ds",
            s3(RAGGED_CSV),
            Map.of("error_mode", "fail_fast")
        );
    }

    // -------- declared mapping -------------------------------------------------------------------

    private void sweepDeclaredMapping() throws IOException {
        crudProbe(
            "declared_mapping",
            "declared column of an undeclarable type",
            "name the column and the unsupported type",
            () -> putDataset("bad_type_ds", "good_ds", s3(GOOD_CSV), null, Map.of("properties", Map.of("id", Map.of("type", "not_a_type"))))
        );
        crudProbe(
            "declared_mapping",
            "unknown key inside the mappings block",
            "name the unknown key",
            () -> putDatasetRaw(
                "bad_mapping_key_ds",
                "{\"data_source\":\"good_ds\",\"resource\":\"" + s3(GOOD_CSV) + "\",\"mappings\":{\"colums\":{}}}"
            )
        );
        crudProbe(
            "declared_mapping",
            "_id.path points at a column that is not declared",
            "name the missing column",
            () -> putDataset(
                "bad_idpath_ds",
                "good_ds",
                s3(GOOD_CSV),
                null,
                Map.of("dynamic", "false", "properties", Map.of("id", Map.of("type", "long")), "_id", Map.of("path", "nonexistent_column"))
            )
        );
        crudProbe(
            "declared_mapping",
            "two declared columns resolving to one physical column",
            "name both logical columns and the physical one they collide on",
            () -> putDataset(
                "rename_collision_ds",
                "good_ds",
                s3(GOOD_CSV),
                null,
                Map.of("properties", Map.of("id", Map.of("type", "long"), "ident", Map.of("type", "long", "path", "id")))
            )
        );
        crudProbe(
            "declared_mapping",
            "date format declared on a non-date column",
            "say format applies only to date columns",
            () -> putDataset(
                "format_on_long_ds",
                "good_ds",
                s3(GOOD_CSV),
                null,
                Map.of("properties", Map.of("id", Map.of("type", "long", "format", "yyyy-MM-dd")))
            )
        );
        crudProbe(
            "declared_mapping",
            "strict declaration with no columns",
            "say strict requires at least one declared column",
            () -> putDataset("empty_strict_ds", "good_ds", s3(GOOD_CSV), null, Map.of("dynamic", "false"))
        );
        // Declared strictly as a type the bytes cannot produce: the failure has to name the column.
        putDataset(
            "strict_mismatch_ds",
            "good_ds",
            s3(GOOD_CSV),
            null,
            Map.of("dynamic", "false", "properties", Map.of("id", Map.of("type", "long"), "city", Map.of("type", "long")))
        );
        queryProbeExisting(
            "declared_mapping",
            "declared type not coercible from the bytes",
            "name the column, the declared type, and the offending value",
            "strict_mismatch_ds"
        );
    }

    // -------- globs ------------------------------------------------------------------------------

    private void sweepGlobs() throws IOException {
        queryProbe(
            "glob",
            "glob matches nothing",
            "say the pattern matched no objects and echo the pattern",
            "glob_empty",
            "good_ds",
            s3("glob/*.avro"),
            null
        );
        queryProbe(
            "glob",
            "glob over incompatible schemas",
            "name the conflicting files and columns",
            "glob_incompatible",
            "good_ds",
            s3("glob/*.csv"),
            null
        );
        queryProbe(
            "glob",
            "glob over a bucket that does not exist",
            "say the bucket does not exist, not that the pattern matched nothing",
            "glob_missing_bucket",
            "good_ds",
            "s3://no-such-bucket-at-all/glob/*.csv",
            null
        );
    }

    // -------- dataset CRUD -----------------------------------------------------------------------

    private void sweepDatasetCrud() throws IOException {
        crudProbe(
            "dataset_crud",
            "put dataset referencing an unknown data source",
            "name the missing data source",
            () -> putDataset("orphan_ds", "no_such_data_source", s3(GOOD_CSV), null, null)
        );
        crudProbe(
            "dataset_crud",
            "put dataset with no resource",
            "say [resource] is required",
            () -> putDatasetRaw("no_resource_ds", "{\"data_source\":\"good_ds\"}")
        );
        crudProbe(
            "dataset_crud",
            "put dataset with an empty resource",
            "say [resource] is required",
            () -> putDataset("empty_resource_ds", "good_ds", "", null, null)
        );
        crudProbe(
            "dataset_crud",
            "put dataset with an unknown top-level field",
            "name the unknown field",
            () -> putDatasetRaw("unknown_field_ds", "{\"data_source\":\"good_ds\",\"resource\":\"" + s3(GOOD_CSV) + "\",\"bogus\":1}")
        );
        crudProbe(
            "dataset_crud",
            "put dataset with malformed JSON",
            "say the body is not valid JSON and where it broke",
            () -> putDatasetRaw("malformed_ds", "{\"data_source\":\"good_ds\",\"resource\":")
        );
        crudProbe(
            "dataset_crud",
            "put dataset whose name contains a comma",
            "say what is wrong with the name",
            () -> putDataset("bad,name", "good_ds", s3(GOOD_CSV), null, null)
        );
        crudProbe(
            "dataset_crud",
            "put dataset whose name is uppercase",
            "say names must be lower case",
            () -> putDataset("UPPER", "good_ds", s3(GOOD_CSV), null, null)
        );
        crudProbe(
            "dataset_crud",
            "put dataset whose name starts with an underscore",
            "say leading underscores are reserved",
            () -> putDataset("_reserved", "good_ds", s3(GOOD_CSV), null, null)
        );
        crudProbe("dataset_crud", "put dataset colliding with an existing index name", "say an index of that name already exists", () -> {
            createPlainIndex("colliding_index");
            putDataset("colliding_index", "good_ds", s3(GOOD_CSV), null, null);
        });
        crudProbe(
            "dataset_crud",
            "put dataset shadowing a secret parent setting",
            "name the shadowed secret setting",
            () -> putDataset("shadow_ds", "good_ds", s3(GOOD_CSV), Map.of("secret_key", "override"), null)
        );
        crudProbe("dataset_crud", "get an unknown dataset", "say the dataset does not exist", () -> get("/_query/dataset/no_such_dataset"));
        crudProbe(
            "dataset_crud",
            "delete an unknown dataset",
            "say the dataset does not exist",
            () -> delete("/_query/dataset/no_such_dataset")
        );
        crudProbe(
            "dataset_crud",
            "query an unknown dataset",
            "say the dataset/index does not exist",
            () -> runEsql("FROM no_such_dataset | LIMIT 1")
        );
    }

    // -------- data source CRUD -------------------------------------------------------------------

    private void sweepDataSourceCrud() throws IOException {
        crudProbe(
            "data_source_crud",
            "put data source with an unknown type",
            "name the unknown type and list the known ones",
            () -> putDataSourceRaw("bad_type", "{\"type\":\"not_a_type\",\"settings\":{}}")
        );
        crudProbe(
            "data_source_crud",
            "put data source with no type",
            "say [type] is required",
            () -> putDataSourceRaw("no_type", "{\"settings\":{}}")
        );
        crudProbe(
            "data_source_crud",
            "put s3 data source with an unknown setting",
            "name the unknown setting and list the accepted ones",
            () -> putDataSource("unknown_setting_ds", Map.of("regionn", "us-east-1"))
        );
        crudProbe(
            "data_source_crud",
            "put s3 data source with anonymous auth plus credentials",
            "say the two are mutually exclusive",
            () -> putDataSource("conflicting_auth_ds", Map.of("auth", "anonymous", "access_key", "k", "secret_key", "s"))
        );
        crudProbe(
            "data_source_crud",
            "put s3 data source with an access key and no secret key",
            "name the missing companion setting",
            () -> putDataSource("half_credentials_ds", Map.of("access_key", "k", "region", "us-east-1"))
        );
        // Credentials supplied so the request gets past the credential check and the endpoint is actually validated;
        // without them the response only ever complains about missing credentials.
        crudProbe(
            "data_source_crud",
            "put s3 data source with a malformed endpoint",
            "say the endpoint is not a valid URL",
            () -> putDataSource(
                "bad_endpoint_ds",
                Map.of("access_key", "k", "secret_key", "s", "region", "us-east-1", "endpoint", "not a url")
            )
        );
        crudProbe(
            "data_source_crud",
            "get an unknown data source",
            "say the data source does not exist",
            () -> get("/_query/data_source/no_such_data_source")
        );
        crudProbe(
            "data_source_crud",
            "delete an unknown data source",
            "say the data source does not exist",
            () -> delete("/_query/data_source/no_such_data_source")
        );
    }

    // -------- referential integrity ---------------------------------------------------------------

    private void sweepReferentialIntegrity() throws IOException {
        putDataSource("doomed_ds", staticCredentialSettings());
        putDataset("doomed_rows", "doomed_ds", s3(GOOD_CSV), null, null);
        crudProbe(
            "referential_integrity",
            "delete a data source that still has datasets",
            "name the datasets that still reference it, or succeed deliberately",
            () -> delete("/_query/data_source/doomed_ds")
        );
        // Drop the dataset first, then the data source, then query the name that no longer resolves. This is the
        // state a user reaches by tidying up in the documented order, so the failure has to be a clean 4xx.
        crudProbe("referential_integrity", "delete a data source after its datasets are gone", "succeed", () -> {
            delete("/_query/dataset/doomed_rows");
            delete("/_query/data_source/doomed_ds");
        });
        queryProbeExisting("referential_integrity", "query a dataset that was deleted", "say the dataset does not exist", "doomed_rows");
    }

    // ---------------------------------------------------------------------------------------------
    // Probe plumbing
    // ---------------------------------------------------------------------------------------------

    @FunctionalInterface
    private interface Action {
        void run() throws IOException;
    }

    /** Registers {@code dataset} against {@code dataSource}, then queries it and records the outcome. */
    private void queryProbe(String group, String name, String expectation, String dataset, String dataSource, String resource, Action setup)
        throws IOException {
        if (setup != null) {
            setup.run();
        }
        record(group, name, expectation, () -> {
            putDataset(dataset, dataSource, resource, null, null);
            runEsql("FROM " + dataset + " | LIMIT 5");
        });
    }

    private void queryProbeWithSettings(
        String group,
        String name,
        String expectation,
        String dataset,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) throws IOException {
        record(group, name, expectation, () -> {
            putDataset(dataset, dataSource, resource, settings, null);
            runEsql("FROM " + dataset + " | LIMIT 5");
        });
    }

    private void queryProbeExisting(String group, String name, String expectation, String dataset) throws IOException {
        record(group, name, expectation, () -> runEsql("FROM " + dataset + " | LIMIT 5"));
    }

    private void crudProbe(String group, String name, String expectation, Action action) throws IOException {
        record(group, name, expectation, action);
    }

    /**
     * Runs {@code action} and records what came back. A probe that succeeds is recorded with status
     * 200 rather than failing the run — an unexpected success is itself a finding worth reading in
     * the report (it usually means a misconfiguration was silently accepted).
     */
    private void record(String group, String name, String expectation, Action action) {
        try {
            action.run();
            probes.add(new Probe(group, name, expectation, 200, "<none>", "<request succeeded>", List.of()));
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            Map<String, Object> body = parseBody(e);
            Map<?, ?> error = body.get("error") instanceof Map<?, ?> m ? m : Map.of();
            probes.add(new Probe(group, name, expectation, status, str(error.get("type")), str(error.get("reason")), flattenCauses(error)));
        } catch (IOException e) {
            probes.add(new Probe(group, name, expectation, -1, e.getClass().getSimpleName(), String.valueOf(e.getMessage()), List.of()));
        }
    }

    private Map<String, Object> parseBody(ResponseException e) {
        try {
            return entityAsMap(e.getResponse());
        } catch (Exception parseFailure) {
            return Map.of();
        }
    }

    /** Flattens the nested {@code caused_by} chain into "type: reason" lines, outermost first. */
    private static List<String> flattenCauses(Map<?, ?> error) {
        List<String> chain = new ArrayList<>();
        Object current = error.get("caused_by");
        while (current instanceof Map<?, ?> cause && chain.size() < 12) {
            chain.add(str(cause.get("type")) + ": " + str(cause.get("reason")));
            current = cause.get("caused_by");
        }
        return chain;
    }

    private static String str(Object o) {
        return o == null ? "<null>" : o.toString();
    }

    // ---------------------------------------------------------------------------------------------
    // REST helpers
    // ---------------------------------------------------------------------------------------------

    private static Map<String, Object> staticCredentialSettings() {
        return Map.of(
            "access_key",
            ACCESS_KEY,
            "secret_key",
            SECRET_KEY,
            "region",
            regionSupplier.get(),
            "endpoint",
            s3HttpFixture.getAddress()
        );
    }

    private static void putDataSource(String name, Map<String, Object> settings) throws IOException {
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", "s3").field("settings", settings).endObject();
            putDataSourceRaw(name, Strings.toString(b));
        }
    }

    private static void putDataSourceRaw(String name, String body) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        req.setJsonEntity(body);
        perform(req);
    }

    /**
     * Every probe goes through here. Warnings are permitted deliberately: a probe must be judged on the
     * status and body it produced, and the default strict handler turns an unrelated deprecation header
     * (e.g. ES|QL's "no limit defined") into a client-side failure that would mask the real outcome.
     */
    private static Response perform(Request req) throws IOException {
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return client().performRequest(req);
    }

    private static void putDataset(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings,
        Map<String, Object> mapping
    ) throws IOException {
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource);
            if (settings != null) {
                b.field("settings", settings);
            }
            if (mapping != null) {
                b.field("mappings", mapping);
            }
            b.endObject();
            putDatasetRaw(name, Strings.toString(b));
        }
    }

    private static void putDatasetRaw(String name, String body) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        req.setJsonEntity(body);
        perform(req);
    }

    private static void get(String path) throws IOException {
        perform(new Request("GET", path));
    }

    private static void delete(String path) throws IOException {
        perform(new Request("DELETE", path));
    }

    private static void createPlainIndex(String name) throws IOException {
        perform(new Request("PUT", "/" + name));
    }

    private static Response runEsql(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":" + quote(query) + "}");
        return perform(req);
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private void assertQuerySucceeds(String query) throws IOException {
        Response r = runEsql(query);
        assertEquals(200, r.getStatusLine().getStatusCode());
    }

    // ---------------------------------------------------------------------------------------------
    // Report + invariants
    // ---------------------------------------------------------------------------------------------

    private Path writeReport() throws IOException {
        Path out = PathUtils.get(System.getProperty("tests.error.report", "build/external-error-surface.md"));
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("# External dataset error surface\n\n");
        sb.append(probes.size())
            .append(" probes across ")
            .append(probes.stream().map(Probe::group).distinct().count())
            .append(" axes.\n\n");
        String currentGroup = null;
        for (Probe p : probes) {
            if (p.group().equals(currentGroup) == false) {
                currentGroup = p.group();
                sb.append("\n## ").append(currentGroup).append("\n");
            }
            sb.append("\n### ").append(p.name()).append("\n\n");
            sb.append("- expected: ").append(p.expectation()).append('\n');
            sb.append("- status: `").append(p.status()).append("`\n");
            sb.append("- type: `").append(p.type()).append("`\n");
            sb.append("- reason: `").append(p.reason()).append("`\n");
            if (p.causeChain().isEmpty() == false) {
                sb.append("- caused_by:\n");
                for (String c : p.causeChain()) {
                    sb.append("  - `").append(c).append("`\n");
                }
            }
        }
        sb.append("\n\n## Reason collisions\n\n");
        Map<String, List<String>> byReason = new TreeMap<>();
        for (Probe p : probes) {
            byReason.computeIfAbsent(normalize(p.reason()), k -> new ArrayList<>()).add(p.group() + "/" + p.name());
        }
        byReason.forEach((reason, cases) -> {
            if (cases.size() > 1) {
                sb.append("\n- `").append(reason).append("` is returned for ").append(cases.size()).append(" distinct conditions:\n");
                cases.forEach(c -> sb.append("  - ").append(c).append('\n'));
            }
        });
        // Not a gate — a reading aid. A deeper cause whose text is missing from the reason is sometimes correct
        // (the reason is already self-sufficient and the cause is a lower-level detail) and sometimes the whole
        // defect. Listing them is how the next person finds the second kind; asserting on them would fail the
        // first kind too.
        sb.append("\n\n## Detail that stayed in the cause chain\n\n");
        for (Probe p : probes) {
            if (p.causeChain().isEmpty()) {
                continue;
            }
            String deepest = p.causeChain().get(p.causeChain().size() - 1);
            String deepestReason = deepest.substring(deepest.indexOf(": ") + 2);
            if (deepestReason.isBlank() == false
                && deepestReason.equals("<null>") == false
                && p.reason().contains(deepestReason) == false) {
                sb.append("\n- **").append(p.group()).append('/').append(p.name()).append("**\n");
                sb.append("  - reason: `").append(p.reason()).append("`\n");
                sb.append("  - deepest cause: `").append(deepestReason).append("`\n");
            }
        }
        Files.writeString(out, sb.toString());
        return out;
    }

    /**
     * Masks object locations so that two probes differing only in <em>which</em> object they addressed still
     * collide: "Failed to resolve metadata for [s3://b/a.parquet]" and "...[s3://b/b.csv]" carry exactly the same
     * information, and counting them as two distinct messages would hide the collapse this suite exists to find.
     * <p>
     * Only URIs are masked, deliberately — not every bracketed token. "Required [resource]" and "Required [type]"
     * name different settings and really are different messages; masking all brackets would fuse them.
     */
    private static String normalize(String reason) {
        return reason.replaceAll("[A-Za-z0-9]+://[^\\s\\]\",]*", "<location>").toLowerCase(Locale.ROOT);
    }

    /** True when every colliding probe belongs to one {@link #SHARED_CONDITIONS} group, i.e. they are one condition. */
    private static boolean isOneCondition(List<String> collidingNames) {
        return SHARED_CONDITIONS.stream().anyMatch(group -> collidingNames.stream().allMatch(group::contains));
    }

    private void assertMatrixInvariants() {
        List<String> violations = new ArrayList<>();

        // 1. Distinct root conditions must not collapse onto one message. Keyed by the reason with object
        // locations masked, since the location is the only part that varies between the collapsed cases.
        Map<String, List<String>> byReason = new LinkedHashMap<>();
        for (Probe p : probes) {
            if (p.status() == 200) {
                continue;
            }
            byReason.computeIfAbsent(normalize(p.reason()), k -> new ArrayList<>()).add(p.name());
        }
        byReason.forEach((reason, cases) -> {
            List<String> unresolved = cases.stream().filter(c -> KNOWN_OPEN.containsKey(c) == false).toList();
            if (unresolved.size() > 1 && isOneCondition(unresolved) == false) {
                violations.add("one reason for " + unresolved.size() + " distinct conditions [" + reason + "]: " + unresolved);
            }
        });

        // 2. No JVM type name in prose. A reason containing "java.lang.IllegalArgumentException:" means some
        // wrapper's message was built from a Throwable#toString rather than from a message someone wrote —
        // the signature of a cause chain that was flattened instead of read.
        for (Probe p : probes) {
            if (p.reason().contains("java.") || p.reason().contains("org.elasticsearch.")) {
                violations.add("JVM type name in reason [" + p.group() + "/" + p.name() + "]: " + p.reason());
            }
        }

        if (violations.isEmpty() == false) {
            StringBuilder sb = new StringBuilder(violations.size() + " error-surface violations:\n");
            violations.forEach(v -> sb.append("  - ").append(v).append('\n'));
            fail(sb.toString());
        }
    }
}
