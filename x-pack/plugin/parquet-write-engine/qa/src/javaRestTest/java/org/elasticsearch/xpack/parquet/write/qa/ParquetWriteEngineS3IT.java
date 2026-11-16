/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.qa;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.hamcrest.Matchers.equalTo;

/**
 * The checks that need a real node: the engine runs in the class loader chain of a plugin that extends the Iceberg plugin, it writes
 * through a real S3 repository (with the conditional writes that the table commit needs), and ES|QL reads the files that the engine wrote.
 */
public class ParquetWriteEngineS3IT extends ESRestTestCase {

    @SuppressForbidden(reason = "the handler of the in-process HTTP server of the S3 fixture")
    private static final class AuthorizedHandler extends S3HttpHandler {
        private final BiPredicate<String, String> authorization;

        AuthorizedHandler(BiPredicate<String, String> authorization) {
            super(S3FixtureUtils.BUCKET, S3FixtureUtils.WAREHOUSE, S3ConsistencyModel.AWS_DEFAULT);
            this.authorization = authorization;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (checkAuthorization(authorization, exchange)) {
                super.handle(exchange);
            }
        }
    }

    /**
     * An S3 fixture that behaves like AWS S3: it honours {@code If-None-Match: *}. The fixture of the ES|QL tests uses the other model
     * ({@link S3ConsistencyModel#STRONG_MPUS}), which ignores conditional writes, and on it two shards that commit to the table at the
     * same moment both write the same version and the later one silently replaces the earlier one. That is what the first run of this
     * test showed, and it is why the table commit needs a store with conditional writes.
     */
    @SuppressForbidden(reason = "the S3 fixture is an in-process HTTP server, and its handler is a com.sun.net.httpserver.HttpHandler")
    private static final class ConditionalWritesS3Fixture extends S3HttpFixture {
        private volatile S3HttpHandler handler;

        ConditionalWritesS3Fixture() {
            super(true, () -> S3ConsistencyModel.AWS_DEFAULT);
        }

        @Override
        protected HttpHandler createHandler() {
            final BiPredicate<String, String> authorization = fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3");
            handler = new AuthorizedHandler(authorization);
            return handler;
        }

        S3HttpHandler getHandler() {
            return handler;
        }

        /** Adds the endpoint and the credentials of the fixture to the {@code EXTERNAL} command of the query. */
        String injectParams(String query) {
            final String trimmed = query.trim();
            final int pipe = FixtureUtils.findFirstPipeAfterExternal(trimmed);
            final String external = pipe == -1 ? trimmed : trimmed.substring(0, pipe).trim();
            final String rest = pipe == -1 ? "" : " " + trimmed.substring(pipe);
            final String entries = "\"endpoint\": \""
                + getAddress()
                + "\", \"access_key\": \""
                + ACCESS_KEY
                + "\", \"secret_key\": \""
                + SECRET_KEY
                + "\"";
            return FixtureUtils.injectWithEntries(external, entries) + rest;
        }
    }

    private static final ConditionalWritesS3Fixture s3Fixture = new ConditionalWritesS3Fixture();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("repository-s3")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        // the S3 client of the repository
        .setting("s3.client.default.endpoint", () -> s3Fixture.getAddress())
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .keystore("s3.client.default.access_key", ACCESS_KEY)
        .keystore("s3.client.default.secret_key", SECRET_KEY)
        .setting("s3.client.default.protocol", "http")
        // do not read the credentials files of the machine that runs the test
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        // the Iceberg plugin reads with Arrow
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .feature(FeatureFlag.ESQL_EXTERNAL_ICEBERG)
        // The local Parquet files are not Lucene files, and a node with assertions on asserts that it knows the extension of every file
        // that the store opens. This is the opt-out that exists for plugins with their own files.
        .systemProperty("es.allow_unknown_lucene_file_extensions", "true")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static final String INDEX = "parquet-s3";
    private static final String BASE_PATH = S3FixtureUtils.WAREHOUSE + "/lake";

    private void put(String path, String json) throws IOException {
        final Request request = new Request("PUT", path);
        request.setJsonEntity(json);
        assertOK(client().performRequest(request));
    }

    private void bulk(int from, int to) throws IOException {
        final StringBuilder body = new StringBuilder();
        for (int i = from; i < to; i++) {
            body.append("{\"index\":{}}\n");
            body.append("{\"@timestamp\":").append(1_700_000_000_000L + i * 1000L);
            body.append(",\"host.name\":\"host-").append(i % 7).append("\",\"count\":").append(i).append(",\"bytes\":").append(i % 1000);
            body.append("}\n");
        }
        final Request request = new Request("POST", "/" + INDEX + "/_bulk");
        request.setJsonEntity(body.toString());
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat("bulk errors: " + response, response.get("errors"), equalTo(false));
    }

    private String indexUuid() throws IOException {
        return indexUuid(INDEX);
    }

    private String indexUuid(String index) throws IOException {
        final Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_settings")));
        @SuppressWarnings("unchecked")
        final Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) response.get(index)).get(
            "settings"
        )).get("index");
        return (String) settings.get("uuid");
    }

    /** Runs an ES|QL query over the table of the index, with the endpoint and the credentials of the fixture. */
    private Map<String, Object> queryTable(String uuid, String suffix, String pipeline) throws IOException {
        return queryTable(INDEX, uuid, suffix, pipeline);
    }

    private Map<String, Object> queryTable(String index, String uuid, String suffix, String pipeline) throws IOException {
        final String table = "s3://" + S3FixtureUtils.BUCKET + "/" + BASE_PATH + "/" + index + "/" + uuid + suffix;
        final String query = s3Fixture.injectParams("EXTERNAL \"" + table + "\" " + pipeline);
        final XContentBuilder body = JsonXContent.contentBuilder().startObject().field("query", query).endObject();
        final Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(body));
        final Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * The commits of the two shards, which flush at the same moment, must all be in the table: consecutive versions (a lost conditional
     * write would overwrite a version), both snapshots in the current metadata, and one data file for each shard in its manifest list.
     */
    private void assertTheTableMetadata(String uuid) throws IOException {
        final String metadataDir = "/" + S3FixtureUtils.BUCKET + "/" + BASE_PATH + "/" + INDEX + "/" + uuid + "/metadata/";
        final var blobs = s3Fixture.getHandler().blobs();
        int latest = 0;
        int count = 0;
        for (String key : blobs.keySet()) {
            if (key.startsWith(metadataDir) && key.matches(".*/v\\d+\\.metadata\\.json")) {
                count++;
                latest = Math.max(latest, Integer.parseInt(key.substring(key.lastIndexOf("/v") + 2, key.indexOf(".metadata.json"))));
            }
        }
        assertThat("metadata versions must be consecutive", count, equalTo(latest));
        assertThat("create, the schema of the columns, and one append for each of the two shards", latest, equalTo(4));
        assertThat(blobs.get(metadataDir + "version-hint.text").contents().utf8ToString(), equalTo(Integer.toString(latest)));
        final String json = blobs.get(metadataDir + "v" + latest + ".metadata.json").contents().utf8ToString();
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            final Map<String, Object> metadata = parser.map();
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> snapshots = (List<Map<String, Object>>) metadata.get("snapshots");
            assertThat("both appends are in the history", snapshots.size(), equalTo(2));
        }
    }

    /**
     * The commits of the table write its entry in the catalog of the repository: a small JSON file below {@code _catalog}, with the
     * {@code s3://} URIs of the table and of the newest metadata file, and that metadata file is in the bucket.
     */
    private void assertTheCatalogEntry(String uuid) throws IOException {
        final var blobs = s3Fixture.getHandler().blobs();
        final String entryKey = "/" + S3FixtureUtils.BUCKET + "/" + BASE_PATH + "/_catalog/default/" + INDEX + ".json";
        assertNotNull("the entry of the table is in the catalog", blobs.get(entryKey));
        try (
            var parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                blobs.get(entryKey).contents().utf8ToString()
            )
        ) {
            final Map<String, Object> entry = parser.map();
            assertThat(entry.get("name"), equalTo(INDEX));
            final String tableRoot = "s3://" + S3FixtureUtils.BUCKET + "/" + BASE_PATH + "/" + INDEX + "/" + uuid;
            assertThat(entry.get("location"), equalTo(tableRoot));
            final String metadata = (String) entry.get("metadata_location");
            assertThat(metadata, org.hamcrest.Matchers.startsWith(tableRoot + "/metadata/v"));
            // a URI s3://bucket/path is the key /bucket/path of the fixture
            assertNotNull("the metadata file that the entry names is in the bucket", blobs.get("/" + metadata.substring("s3://".length())));
            // the entry names the newest version, or the one before it when two shards committed at the same moment
            final int named = ((Number) entry.get("metadata_version")).intValue();
            assertFalse(
                "no metadata file two versions after the one in the entry",
                blobs.containsKey("/" + tableRoot.substring("s3://".length()) + "/metadata/v" + (named + 2) + ".metadata.json")
            );
        }
    }

    public void testTheEngineWritesATableThatEsqlReads() throws Exception {
        put(
            "/_snapshot/lake",
            "{\"type\":\"s3\",\"settings\":{\"bucket\":\""
                + S3FixtureUtils.BUCKET
                + "\",\"base_path\":\""
                + BASE_PATH
                + "\","
                + "\"path_style_access\":true}}"
        );
        put("/" + INDEX, """
            {
              "settings": {
                "index.storage_format": "parquet",
                "index.mode": "columnar",
                "index.number_of_shards": 2,
                "index.parquet.repository": "lake"
              },
              "mappings": {
                "dynamic": "strict",
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host.name": { "type": "keyword" },
                  "count": { "type": "long" },
                  "bytes": { "type": "integer" }
                }
              }
            }
            """);
        ensureGreen(INDEX);
        bulk(0, 3_000);
        bulk(3_000, 5_000);
        final Map<String, Object> flush = entityAsMap(client().performRequest(new Request("POST", "/" + INDEX + "/_flush")));
        @SuppressWarnings("unchecked")
        final Map<String, Object> shards = (Map<String, Object>) flush.get("_shards");
        assertThat(shards.get("failed"), equalTo(0));

        assertTheTableMetadata(indexUuid());
        assertTheCatalogEntry(indexUuid());

        // The data files below the table: ES|QL reads the Parquet files that the engine uploaded with its Parquet reader. Reading the
        // table through its Iceberg metadata is not possible yet, see the findings of the plan.
        final Map<String, Object> result = queryTable(
            indexUuid(),
            "/data/*/*/*.parquet",
            "| STATS c = COUNT(*), s = SUM(count), lo = MIN(count), hi = MAX(count) | LIMIT 10"
        );

        @SuppressWarnings("unchecked")
        final List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values.size(), equalTo(1));
        assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(5_000L));
        assertThat(((Number) values.get(0).get(1)).longValue(), equalTo(5_000L * 4_999L / 2));
        assertThat(((Number) values.get(0).get(2)).longValue(), equalTo(0L));
        assertThat(((Number) values.get(0).get(3)).longValue(), equalTo(4_999L));
    }

    /**
     * A file that is larger than {@code buffer_size} goes to S3 as a multipart upload: an initiate request, the parts, and a complete
     * request. The metadata files of the table are small and go as single requests. The table must list the file and ES|QL must read all
     * its rows, which shows that the parts were put together in the right order.
     */
    public void testAFileLargerThanTheBufferIsUploadedInParts() throws Exception {
        final String index = "parquet-s3-big";
        put(
            "/_snapshot/lake",
            "{\"type\":\"s3\",\"settings\":{\"bucket\":\""
                + S3FixtureUtils.BUCKET
                + "\",\"base_path\":\""
                + BASE_PATH
                + "\",\"path_style_access\":true,\"buffer_size\":\"5mb\"}}"
        );
        put("/" + index, """
            {
              "settings": {
                "index.storage_format": "parquet",
                "index.mode": "columnar",
                "index.number_of_shards": 1,
                "index.parquet.repository": "lake",
                "index.parquet.compaction.enabled": false
              },
              "mappings": {
                "dynamic": "strict",
                "properties": {
                  "count": { "type": "long" },
                  "a": { "type": "keyword" },
                  "b": { "type": "keyword" },
                  "c": { "type": "keyword" },
                  "d": { "type": "keyword" }
                }
              }
            }
            """);
        ensureGreen(index);
        final int bulks = 30;
        final int perBulk = 5_000;
        final java.util.Random random = random();
        for (int bulk = 0; bulk < bulks; bulk++) {
            final StringBuilder body = new StringBuilder();
            for (int i = 0; i < perBulk; i++) {
                body.append("{\"index\":{}}\n{\"count\":").append(bulk * perBulk + i);
                for (String field : new String[] { "a", "b", "c", "d" }) {
                    // 48 hexadecimal characters of noise: they compress to about half, so the file stays larger than the buffer
                    body.append(",\"").append(field).append("\":\"");
                    for (int part = 0; part < 3; part++) {
                        body.append(String.format(java.util.Locale.ROOT, "%016x", random.nextLong()));
                    }
                    body.append('"');
                }
                body.append("}\n");
            }
            final Request request = new Request("POST", "/" + index + "/_bulk");
            request.setJsonEntity(body.toString());
            final Map<String, Object> response = entityAsMap(client().performRequest(request));
            assertThat("bulk errors: " + response.get("errors"), response.get("errors"), equalTo(false));
        }
        final Map<String, Object> flush = entityAsMap(client().performRequest(new Request("POST", "/" + index + "/_flush")));
        @SuppressWarnings("unchecked")
        final Map<String, Object> shards = (Map<String, Object>) flush.get("_shards");
        assertThat(shards.get("failed"), equalTo(0));

        // the data file is larger than the buffer, and it was uploaded in parts
        final var blobs = s3Fixture.getHandler().blobs();
        String dataKey = null;
        for (String key : blobs.keySet()) {
            if (key.contains("/" + index + "/") && key.contains("/data/shard-0/") && key.endsWith(".parquet")) {
                assertNull("one data file", dataKey);
                dataKey = key;
            }
        }
        assertNotNull("the engine uploaded a data file", dataKey);
        assertThat("larger than buffer_size", blobs.get(dataKey).contents().length() > 5 * 1024 * 1024, equalTo(true));
        final String key = dataKey;
        final long posts = s3Fixture.getHandler()
            .requestLog()
            .stream()
            .filter(r -> r.method().equals("POST") && r.path().equals(key))
            .count();
        final long parts = s3Fixture.getHandler()
            .requestLog()
            .stream()
            .filter(r -> r.method().equals("PUT") && r.path().equals(key))
            .count();
        assertThat("an initiate and a complete request for the multipart upload", posts, equalTo(2L));
        assertThat("at least two parts", parts >= 2, equalTo(true));

        // the rows are all there, in the right order
        final String uuid = indexUuid(index);
        final Map<String, Object> result = queryTable(
            index,
            uuid,
            "/data/*/*/*.parquet",
            "| STATS c = COUNT(*), lo = MIN(count), hi = MAX(count), s = SUM(count), n = COUNT(d) | LIMIT 10"
        );
        @SuppressWarnings("unchecked")
        final List<Object> row = ((List<List<Object>>) result.get("values")).get(0);
        final long total = (long) bulks * perBulk;
        assertThat(((Number) row.get(0)).longValue(), equalTo(total));
        assertThat(((Number) row.get(1)).longValue(), equalTo(0L));
        assertThat(((Number) row.get(2)).longValue(), equalTo(total - 1));
        assertThat(((Number) row.get(3)).longValue(), equalTo(total * (total - 1) / 2));
        assertThat(((Number) row.get(4)).longValue(), equalTo(total));
    }
}
