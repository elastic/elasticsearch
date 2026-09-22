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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that no user — regardless of privilege level — can see the storage location (S3 bucket
 * name, object key prefix) through error messages or profile plan strings. The object name (last
 * path segment, e.g. {@code good.csv}) is always shown to everyone; the bucket and prefix are
 * never shown to anyone.
 *
 * <p>Four failing shapes are tested:
 * <ol>
 *   <li>Single-object access-denied (S3 403) — exercises the storage-object path.</li>
 *   <li>Glob listing access-denied (S3 403 on the list call) — exercises the storage-provider path.</li>
 *   <li>Format mismatch (garbage bytes read as Parquet) — exercises the format-reader path.</li>
 *   <li>ORC tail-parsing failure (garbage bytes with .orc extension) — exercises the ORC reader path.</li>
 * </ol>
 *
 * <p>A fifth scenario verifies that successful queries issued with {@code profile:true} do not expose
 * the bucket name in {@code profile.plans[].plan} strings for any user, while still preserving the
 * plan shape (e.g. {@code ExternalSourceExec} node type).
 *
 * <p>The cluster uses two nodes so that scan-time failures exercise the cross-node error path.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DatasetLocationSecurityIT extends ESRestTestCase {

    private static final String BUCKET = "ds-loc-security-bucket";
    private static final String ACCESS_KEY = "ds_loc_access_key";
    private static final String SECRET_KEY = "ds_loc_secret_key";

    private static final DynamicRegionSupplier regionSupplier = new DynamicRegionSupplier();

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(
        BUCKET,
        AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    private static final String ENCRYPTION_PASSWORD_ID = "test";

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "ds-loc-security-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .rolesFile(Resource.fromClasspath("dataset_location_security_roles.yml"))
        .user("ds-loc-admin", "ds-loc-admin-pass", "ds_loc_admin", true)
        .user("ds-loc-reader", "ds-loc-reader-pass", "ds_loc_reader", false)
        .user("ds-loc-metadata-reader", "ds-loc-metadata-reader-pass", "ds_loc_metadata_reader", false)
        .environment("AWS_REGION", regionSupplier)
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("ds-loc-admin", new SecureString("ds-loc-admin-pass".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @BeforeClass
    public static void skipForRelease() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    private static final String GOOD_CSV = "loc/good.csv";
    private static final String DENIED_CSV = "loc/denied.csv";
    private static final String GARBAGE_PARQUET = "loc/garbage.parquet";
    private static final String GARBAGE_ORC = "loc/garbage.orc";
    private static final String GLOB_A = "loc/glob/a.csv";
    private static final String GLOB_B = "loc/glob/b.csv";

    @BeforeClass
    public static void seedFixture() {
        s3HttpFixture.seedBlob(GOOD_CSV, "id,name\n1,alpha\n2,beta\n".getBytes(StandardCharsets.UTF_8));
        s3HttpFixture.seedBlob(DENIED_CSV, "id,name\n1,alpha\n".getBytes(StandardCharsets.UTF_8));
        s3HttpFixture.seedBlob(GLOB_A, "id,name\n1,alpha\n".getBytes(StandardCharsets.UTF_8));
        s3HttpFixture.seedBlob(GLOB_B, "id,name\n2,beta\n".getBytes(StandardCharsets.UTF_8));
        // Garbage bytes that will fail Parquet parsing (no valid magic bytes).
        s3HttpFixture.seedBlob(
            GARBAGE_PARQUET,
            "this is plain text pretending to be a parquet file, repeated to give it some length.\n".repeat(8)
                .getBytes(StandardCharsets.UTF_8)
        );
        // Garbage bytes that will fail ORC tail parsing (no valid PostScript or magic).
        s3HttpFixture.seedBlob(
            GARBAGE_ORC,
            "this is plain text pretending to be an orc file, repeated to give it some length.\n".repeat(8).getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Verifies that the bucket name never appears in error responses or profile plan strings for
     * any user. The object name (last path segment) may appear, but the bucket and prefix must not.
     */
    public void testDatasetLocationIsNeverExposed() throws IOException {
        // Data source that serves all seeded objects normally.
        putDataSource(
            "ds_loc_good_src",
            Map.of(
                "access_key",
                ACCESS_KEY,
                "secret_key",
                SECRET_KEY,
                "region",
                regionSupplier.get(),
                "endpoint",
                s3HttpFixture.getAddress()
            )
        );
        // Data source with wrong credentials — every read and every listing returns S3 403.
        putDataSource(
            "ds_loc_bad_src",
            Map.of(
                "access_key",
                "ds_loc_wrong_key",
                "secret_key",
                SECRET_KEY,
                "region",
                regionSupplier.get(),
                "endpoint",
                s3HttpFixture.getAddress()
            )
        );

        // Shape 1: single object, access denied (S3 403 on the object read).
        putDataset("ds_loc_denied_single", "ds_loc_bad_src", s3(DENIED_CSV), null);
        // Shape 2: glob, listing access denied (S3 403 on the listing call).
        putDataset("ds_loc_denied_glob", "ds_loc_bad_src", s3("loc/glob/*.csv"), null);
        // Shape 3: single object, format mismatch (garbage bytes with a .parquet extension).
        putDataset("ds_loc_wrong_format", "ds_loc_good_src", s3(GARBAGE_PARQUET), null);
        // Shape 4: single object, ORC tail parsing failure (garbage bytes with an .orc extension).
        // Verifies that OrcFormatReader does not embed the full storage path in its error message.
        putDataset("ds_loc_bad_orc", "ds_loc_good_src", s3(GARBAGE_ORC), null);
        // Good dataset for the profile-plan test.
        putDataset("ds_loc_good", "ds_loc_good_src", s3(GOOD_CSV), null);

        // ── Failing shapes: bucket hidden, object name visible ───────────────────────────────────

        // For single-object datasets the object name (last path segment) must appear in every
        // user's error. For the glob dataset the listing fails before any object is identified,
        // so there is no object name to assert.
        Map<String, String> expectedObjectName = Map.of(
            "ds_loc_denied_single",
            "denied.csv",
            "ds_loc_wrong_format",
            "garbage.parquet",
            "ds_loc_bad_orc",
            "garbage.orc"
        );

        for (String dataset : List.of("ds_loc_denied_single", "ds_loc_denied_glob", "ds_loc_wrong_format", "ds_loc_bad_orc")) {
            for (String[] userAndPass : new String[][] { { "ds-loc-reader", "reader" }, { "ds-loc-metadata-reader", "metadata_reader" } }) {
                String user = userAndPass[0];
                String label = userAndPass[1];
                ResponseException error = expectThrows(ResponseException.class, () -> runEsqlAs(user, "FROM " + dataset + " | LIMIT 5"));
                List<String> texts = allErrorText(entityAsMap(error.getResponse()));
                for (String text : texts) {
                    assertThat(label + " must not see bucket name in error for [" + dataset + "]", text, not(containsString(BUCKET)));
                }
                String objName = expectedObjectName.get(dataset);
                if (objName != null) {
                    assertThat(
                        label + " must see object name in error for [" + dataset + "]",
                        texts.stream().anyMatch(t -> t.contains(objName)),
                        is(true)
                    );
                }
            }
        }

        // ── Profile plan strings: neither user sees the bucket name ──────────────────────────────

        for (String[] userAndPass : new String[][] { { "ds-loc-reader", "reader" }, { "ds-loc-metadata-reader", "metadata_reader" } }) {
            String user = userAndPass[0];
            String label = userAndPass[1];
            Map<String, Object> profileResp = runEsqlWithProfileAs(user, "FROM ds_loc_good | LIMIT 5");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> plans = plansFromProfileResponse(profileResp);
            for (Map<String, Object> entry : plans) {
                String plan = (String) entry.get("plan");
                if (plan != null) {
                    assertThat(label + " must not see bucket name in profile plan", plan, not(containsString(BUCKET)));
                    // Confirm the plan shape (node type) is still present.
                    assertThat("plan shape must be visible to " + label, plan, containsString("ExternalSourceExec"));
                }
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────────────

    private static String s3(String key) {
        return "s3://" + BUCKET + "/" + key;
    }

    private static void putDataSource(String name, Map<String, Object> settings) throws IOException {
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", "s3").field("settings", settings).endObject();
            Request req = new Request("PUT", "/_query/data_source/" + name);
            req.setJsonEntity(Strings.toString(b));
            req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            client().performRequest(req);
        }
    }

    private static void putDataset(String name, String dataSource, String resource, Map<String, Object> extraSettings) throws IOException {
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource);
            Map<String, Object> settings = new HashMap<>();
            settings.put("region", regionSupplier.get());
            if (extraSettings != null) {
                settings.putAll(extraSettings);
            }
            b.field("settings", settings);
            b.endObject();
            Request req = new Request("PUT", "/_query/dataset/" + name);
            req.setJsonEntity(Strings.toString(b));
            req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            client().performRequest(req);
        }
    }

    private void runEsqlAs(String username, String query) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":" + quote(query) + "}");
        req.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", username).setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        client().performRequest(req);
    }

    private Map<String, Object> runEsqlWithProfileAs(String username, String query) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":" + quote(query) + ",\"profile\":true}");
        req.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", username).setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        Response resp = client().performRequest(req);
        return entityAsMap(resp);
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> plansFromProfileResponse(Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("response must contain a profile", profile);
        List<Map<String, Object>> plans = (List<Map<String, Object>>) profile.get("plans");
        assertNotNull("profile must contain a plans list", plans);
        return plans;
    }

    /** Collects every string value in the response map (recursively through nested maps and lists). */
    private static List<String> allErrorText(Map<?, ?> responseMap) {
        List<String> texts = new ArrayList<>();
        collectTexts(responseMap, texts);
        return texts;
    }

    private static void collectTexts(Object node, List<String> out) {
        if (node instanceof String s) {
            out.add(s);
        } else if (node instanceof Map<?, ?> m) {
            m.values().forEach(v -> collectTexts(v, out));
        } else if (node instanceof List<?> l) {
            l.forEach(item -> collectTexts(item, out));
        }
    }
}
