/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import io.netty.handler.codec.http.HttpMethod;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.cluster.util.Version;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RollingUpgradeDeprecatedSettingsIT extends RollingUpgradeIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public RollingUpgradeDeprecatedSettingsIT(List<Version> nodesVersions) {
        super(nodesVersions);
    }

    /**
     * Creates an index on N-2, upgrades to N -1 and marks as read-only, then remains searchable during rolling upgrades.
     */
    @SuppressWarnings("deprecation")
    public void testIndexUpgrade() throws Exception {
        final String index = suffix("index-rolling-upgraded");
        final int numDocs = 2543;

        // setup index with deprecated index settings on N-2
        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            createIndexLenient(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    // add some index settings deprecated in v7
                    .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), true)
                    .put(IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey(), 100)
                    .put(Store.FORCE_RAM_TERM_DICT.getKey(), false)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)
                    .build()
            );
            indexDocs(index, numDocs);
            return;
        }
        assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
        ensureGreen(index);

        if (isIndexClosed(index) == false) {
            assertDocCount(client(), index, numDocs);
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            final var maybeClose = randomBoolean();
            if (maybeClose) {
                logger.debug("--> closing index [{}] before upgrade", index);
                closeIndex(index);
            }

            addAndAssertIndexBlocks(index, maybeClose);
            return;
        }

        if (nodesVersions().values().stream().anyMatch(v -> v.onOrAfter(VERSION_CURRENT))) {
            final var isClosed = isIndexClosed(index);
            assertAndModifyIndexBlocks(index, isClosed);
            ensureWriteBlock(index, isClosed);

            if (isClosed) {
                logger.debug("--> re-opening index [{}] after upgrade", index);
                openIndex(index);
                ensureGreen(index);
            }

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            updateRandomIndexSettings(index);
            updateRandomMappings(index);

            if (randomBoolean()) {
                logger.debug("--> random closing of index [{}] before upgrade", index);
                closeIndex(index);
                ensureGreen(index);
            }
        }
    }

    private static void createIndexLenient(RestClient client, String name, Settings settings) throws IOException {
        final Request request = newXContentRequest(HttpMethod.PUT, "/" + name, (builder, params) -> {
            builder.startObject("settings");
            settings.toXContent(builder, params);
            builder.endObject();
            return builder;
        });
        client.performRequest(request);
    }

    /**
     * Builds a REST client that will tolerate warnings in the response headers. The default
     * is to throw an exception.
     */
    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }
}
