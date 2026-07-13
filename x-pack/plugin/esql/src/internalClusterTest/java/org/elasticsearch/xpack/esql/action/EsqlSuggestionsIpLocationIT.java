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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

/**
 * Exercises {@code IP_LOCATION} field-name completion end to end through the real
 * {@link TransportEsqlSuggestionsAction}. Unlike {@link IpLocationEsqlConsumerLifecycleIT},
 * this doesn't need the download/sentinel lifecycle at all (suggestions only resolves schema, never reads
 * document data on this path) — the databases are placed directly on {@code ingest.geoip.database_path}
 * with the downloader disabled, exactly the "config databases, no download" shape
 * {@link org.elasticsearch.ingest.geoip.AbstractGeoIpIT} itself uses before its download-specific tests run.
 */
public class EsqlSuggestionsIpLocationIT extends AbstractEsqlIntegTestCase {

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

        assertThat(response.fields(), hasKey("g.country_iso_code"));
        assertThat(response.fields(), hasKey("g.city_name"));
        assertThat(response.fields().get("g.country_iso_code").type(), equalTo("keyword"));
        assertThat(response.fields().get("g.city_name").type(), equalTo("keyword"));
    }
}
