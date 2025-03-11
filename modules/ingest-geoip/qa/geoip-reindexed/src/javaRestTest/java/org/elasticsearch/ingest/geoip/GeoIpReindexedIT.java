/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.geoip;

import fixture.geoip.GeoIpHttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class GeoIpReindexedIT extends ParameterizedFullClusterRestartTestCase {

    private static final boolean useFixture = Boolean.getBoolean("geoip_use_service") == false;

    private static final GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    // e.g. use ./gradlew -Dtests.jvm.argline="-Dgeoip_test_with_security=false" ":modules:ingest-geoip:qa:full-cluster-restart:check"
    // to set this to false, if you so desire
    private static final boolean useSecurity = Boolean.parseBoolean(System.getProperty("geoip_test_with_security", "true"));

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(Version.fromString(OLD_CLUSTER_VERSION))
        .nodes(2)
        .setting("ingest.geoip.downloader.endpoint", () -> fixture.getAddress(), s -> useFixture)
        .setting("xpack.security.enabled", useSecurity ? "true" : "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .build();

    @Override
    protected Settings restClientSettings() {
        Settings settings = super.restClientSettings();
        if (useSecurity) {
            String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
            settings = Settings.builder().put(settings).put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
        return settings;
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    public GeoIpReindexedIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testGeoIpSystemFeaturesMigration() throws Exception {
        final List<String> maybeSecurityIndex = useSecurity ? List.of(".security-7") : List.of();
        final List<String> maybeSecurityIndexReindexed = useSecurity ? List.of(".security-7-reindexed-for-10") : List.of();

        if (isRunningAgainstOldCluster()) {
            Request enableDownloader = new Request("PUT", "/_cluster/settings");
            enableDownloader.setJsonEntity("""
                {"persistent": {"ingest.geoip.downloader.enabled": true}}
                """);
            assertOK(client().performRequest(enableDownloader));

            Request putPipeline = new Request("PUT", "/_ingest/pipeline/geoip");
            putPipeline.setJsonEntity("""
                {
                    "description": "Add geoip info",
                    "processors": [{
                        "geoip": {
                            "field": "ip",
                            "target_field": "geo",
                            "database_file": "GeoLite2-Country.mmdb"
                        }
                    }]
                }
                """);
            assertOK(client().performRequest(putPipeline));

            // wait for the geo databases to all be loaded
            assertBusy(() -> testDatabasesLoaded(), 30, TimeUnit.SECONDS);

            // the geoip index should be created
            assertBusy(() -> testCatIndices(List.of(".geoip_databases"), maybeSecurityIndex));
            assertBusy(() -> testIndexGeoDoc());

            // before the upgrade, Kibana should work
            assertBusy(() -> testGetStarAsKibana(List.of("my-index-00001"), maybeSecurityIndex));

            // as should a normal get *
            assertBusy(() -> testGetStar(List.of("my-index-00001"), maybeSecurityIndex));

            // and getting data streams
            assertBusy(() -> testGetDatastreams());
        } else {
            // after the upgrade, but before the migration, Kibana should work
            assertBusy(() -> testGetStarAsKibana(List.of("my-index-00001"), maybeSecurityIndex));

            // as should a normal get *
            assertBusy(() -> testGetStar(List.of("my-index-00001"), maybeSecurityIndex));

            // and getting data streams
            assertBusy(() -> testGetDatastreams());

            // migrate the system features and give the cluster a moment to settle
            Request migrateSystemFeatures = new Request("POST", "/_migration/system_features");
            assertOK(client().performRequest(migrateSystemFeatures));
            ensureHealth(request -> request.addParameter("wait_for_status", "yellow"));

            assertBusy(() -> testCatIndices(List.of(".geoip_databases-reindexed-for-10", "my-index-00001"), maybeSecurityIndexReindexed));
            assertBusy(() -> testIndexGeoDoc());

            // after the migration, Kibana should work
            assertBusy(() -> testGetStarAsKibana(List.of("my-index-00001"), maybeSecurityIndexReindexed));

            // as should a normal get *
            assertBusy(() -> testGetStar(List.of("my-index-00001"), maybeSecurityIndexReindexed));

            // and getting data streams
            assertBusy(() -> testGetDatastreams());

            Request disableDownloader = new Request("PUT", "/_cluster/settings");
            disableDownloader.setJsonEntity("""
                {"persistent": {"ingest.geoip.downloader.enabled": false}}
                """);
            assertOK(client().performRequest(disableDownloader));

            // the geoip index should be deleted
            assertBusy(() -> testCatIndices(List.of("my-index-00001"), maybeSecurityIndexReindexed));

            Request enableDownloader = new Request("PUT", "/_cluster/settings");
            enableDownloader.setJsonEntity("""
                {"persistent": {"ingest.geoip.downloader.enabled": true}}
                """);
            assertOK(client().performRequest(enableDownloader));

            // wait for the geo databases to all be loaded
            assertBusy(() -> testDatabasesLoaded(), 30, TimeUnit.SECONDS);

            // the geoip index should be recreated
            assertBusy(() -> testCatIndices(List.of(".geoip_databases", "my-index-00001"), maybeSecurityIndexReindexed));
            assertBusy(() -> testIndexGeoDoc());
        }
    }

    @SuppressWarnings("unchecked")
    private void testDatabasesLoaded() throws IOException {
        Request getTaskState = new Request("GET", "/_cluster/state");
        ObjectPath state = ObjectPath.createFromResponse(assertOK(client().performRequest(getTaskState)));

        List<?> tasks = state.evaluate("metadata.persistent_tasks.tasks");
        // Short-circuit to avoid using steams if the list is empty
        if (tasks.isEmpty()) {
            fail();
        }
        Map<String, Object> databases = (Map<String, Object>) tasks.stream().map(task -> {
            try {
                return ObjectPath.evaluate(task, "task.geoip-downloader.state.databases");
            } catch (IOException e) {
                return null;
            }
        }).filter(Objects::nonNull).findFirst().orElse(null);

        assertNotNull(databases);

        for (String name : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")) {
            Object database = databases.get(name);
            assertNotNull(database);
            assertNotNull(ObjectPath.evaluate(database, "md5"));
        }
    }

    private void testCatIndices(List<String> indexNames, @Nullable List<String> additionalIndexNames) throws IOException {
        Request catIndices = new Request("GET", "_cat/indices/*?s=index&h=index&expand_wildcards=all");
        // the cat APIs can sometimes 404, erroneously
        // see https://github.com/elastic/elasticsearch/issues/104371
        setIgnoredErrorResponseCodes(catIndices, RestStatus.NOT_FOUND);
        String response = EntityUtils.toString(assertOK(client().performRequest(catIndices)).getEntity());
        List<String> indices = List.of(response.trim().split("\\s+"));

        if (additionalIndexNames != null) {
            indexNames = CollectionUtils.concatLists(indexNames, additionalIndexNames);
        }

        assertThat(new HashSet<>(indices), is(new HashSet<>(indexNames)));
    }

    private void testIndexGeoDoc() throws IOException {
        Request putDoc = new Request("PUT", "/my-index-00001/_doc/my_id?pipeline=geoip");
        putDoc.setJsonEntity("""
            {"ip": "89.160.20.128"}
            """);
        assertOK(client().performRequest(putDoc));

        Request getDoc = new Request("GET", "/my-index-00001/_doc/my_id");
        ObjectPath doc = ObjectPath.createFromResponse(assertOK(client().performRequest(getDoc)));
        assertNull(doc.evaluate("_source.tags"));
        assertEquals("Sweden", doc.evaluate("_source.geo.country_name"));
    }

    private void testGetStar(List<String> indexNames, @Nullable List<String> additionalIndexNames) throws IOException {
        Request getStar = new Request("GET", "*?expand_wildcards=all");
        getStar.setOptions(
            RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE) // we don't care about warnings, just errors
        );
        Response response = assertOK(client().performRequest(getStar));

        if (additionalIndexNames != null) {
            indexNames = CollectionUtils.concatLists(indexNames, additionalIndexNames);
        }

        Map<String, Object> map = responseAsMap(response);
        assertThat(map.keySet(), is(new HashSet<>(indexNames)));
    }

    private void testGetStarAsKibana(List<String> indexNames, @Nullable List<String> additionalIndexNames) throws IOException {
        Request getStar = new Request("GET", "*?expand_wildcards=all");
        getStar.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("X-elastic-product-origin", "kibana")
                .setWarningsHandler(WarningsHandler.PERMISSIVE) // we don't care about warnings, just errors
        );
        Response response = assertOK(client().performRequest(getStar));

        if (additionalIndexNames != null) {
            indexNames = CollectionUtils.concatLists(indexNames, additionalIndexNames);
        }

        Map<String, Object> map = responseAsMap(response);
        assertThat(map.keySet(), is(new HashSet<>(indexNames)));
    }

    private void testGetDatastreams() throws IOException {
        final List<List<String>> wildcardOptions = List.of(
            List.of(), // the default for expand_wildcards (that is, the option is not specified)
            List.of("all"),
            List.of("none"),
            List.of("hidden"),
            List.of("open"),
            List.of("closed"),
            List.of("hidden", "open"),
            List.of("hidden", "closed"),
            List.of("open", "closed")
        );
        for (List<String> expandWildcards : wildcardOptions) {
            final Request getStar = new Request(
                "GET",
                "_data_stream" + (expandWildcards.isEmpty() ? "" : ("?expand_wildcards=" + String.join(",", expandWildcards)))
            );
            getStar.setOptions(
                RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE) // we only care about errors
            );
            Response response = client().performRequest(getStar);
            assertOK(response);

            // note: we don't actually care about the response, just that there was one and that it didn't error out on us
        }
    }
}
