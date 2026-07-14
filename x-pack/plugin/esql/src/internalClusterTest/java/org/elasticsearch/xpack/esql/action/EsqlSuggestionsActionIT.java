/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.ingest.geoip.AbstractGeoIpIT.IngestGeoIpSettingsPlugin;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.ingest.geoip.GeoIpTestUtils;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.plugins.Plugin;

import java.nio.file.Path;
import java.util.Collection;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Exercises {@link TransportEsqlSuggestionsAction} through the real transport action against a real
 * cluster: field-name completion resolves a plain index's actual mapped fields/types rather than a stub
 * schema, a {@code remote:}-qualified target falls back to the coordinator-only skeleton, and
 * {@code IP_LOCATION}'s dotted output fields resolve with correct wire types (see
 * {@code TransportEsqlSuggestionsActionTests} for the coordinator-only, unanalyzed fallback path).
 *
 * <p>The {@code IP_LOCATION} case needs the geoip plugin and a database path configured on every node;
 * that setup is inert for the other two tests, so it's shared across the whole class rather than split
 * into a separate IT.
 */
public class EsqlSuggestionsActionIT extends AbstractEsqlIntegTestCase {

    private static Path databasePath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), IngestGeoIpPlugin.class),
            IngestGeoIpSettingsPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (databasePath == null) {
            databasePath = createTempDir();
            GeoIpTestUtils.copyDefaultDatabases(databasePath);
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("ingest.geoip.database_path", databasePath.toString())
            .put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testFieldNameCompletionResolvesRealIndexSchema() {
        assertAcked(client().admin().indices().prepareCreate("suggestions_test").setMapping("value", "type=long", "name", "type=keyword"));
        client().prepareIndex("suggestions_test").setSource("value", 1, "name", "a").get();
        client().admin().indices().prepareRefresh("suggestions_test").get();

        String query = "FROM suggestions_test | KEEP *";
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(query.length());
        EsqlSuggestionsResponse response = client().execute(EsqlSuggestionsAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);

        assertMap(
            response.fields(),
            matchesMap().entry("value", FieldSuggestion.ofType("long")).entry("name", FieldSuggestion.ofType("keyword"))
        );
    }

    public void testRemoteQualifiedTargetFallsBackToCoordinatorOnlyWithoutError() {
        String query = "FROM remote_cluster:suggestions_test | KEEP val*";
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(query.length());
        // No error, no attempted remote-cluster resolution: falls back to the parse-only skeleton.
        EsqlSuggestionsResponse response = client().execute(EsqlSuggestionsAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);
        assertMap(response.fields(), matchesMap());
        assertThat(response.warnings(), matchesList());
    }

    /**
     * Uses {@code KEEP g.*} (rather than naming a single dotted field) so the optimizer's column
     * pruning keeps every IP_LOCATION output field in scope — pinning down the "dotted output
     * fields" assertion regardless of which one happens to sit under the cursor.
     */
    public void testIpLocationFieldNameCompletionResolvesDottedOutputFields() {
        String query = "ROW ip = \"89.160.20.128\" | IP_LOCATION g = ip | KEEP g.*";
        int cursor = query.indexOf("g.*") + 1; // on the field-name slot after IP_LOCATION, not a literal
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(cursor);
        EsqlSuggestionsResponse response = client().execute(EsqlSuggestionsAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);

        assertMap(
            response.fields(),
            matchesMap().extraOk()
                .entry("g.country_iso_code", FieldSuggestion.ofType("keyword"))
                .entry("g.city_name", FieldSuggestion.ofType("keyword"))
        );
    }
}
