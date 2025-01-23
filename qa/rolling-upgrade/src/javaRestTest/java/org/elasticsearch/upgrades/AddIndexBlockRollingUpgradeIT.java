/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class AddIndexBlockRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String INDEX_NAME = "test_add_block";

    public AddIndexBlockRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testAddBlock() throws Exception {
        if (isOldCluster()) {
            createIndex(INDEX_NAME);
        } else if (isMixedCluster()) {
            blockWrites();
            // this is used both for upgrading from 9.0.0 to current and from 8.18 to current.
            if (minimumTransportVersion().before(TransportVersions.ADD_INDEX_BLOCK_TWO_PHASE)) {
                assertNull(verifiedSettingValue());
            } else {
                assertThat(verifiedSettingValue(), Matchers.equalTo("true"));

                expectThrows(
                    ResponseException.class,
                    () -> updateIndexSettings(
                        INDEX_NAME,
                        Settings.builder().putNull(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey())
                    )
                );
            }
        } else {
            assertTrue(isUpgradedCluster());
            blockWrites();
            assertThat(verifiedSettingValue(), Matchers.equalTo("true"));

            expectThrows(
                ResponseException.class,
                () -> updateIndexSettings(
                    INDEX_NAME,
                    Settings.builder().putNull(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey())
                )
            );
        }
    }

    private static void blockWrites() throws IOException {
        var block = randomFrom(IndexMetadata.APIBlock.READ_ONLY, IndexMetadata.APIBlock.WRITE).name().toLowerCase(Locale.ROOT);
        client().performRequest(new Request(HttpMethod.PUT.name(), "/" + INDEX_NAME + "/_block/" + block));

        expectThrows(
            ResponseException.class,
            () -> client().performRequest(
                newXContentRequest(HttpMethod.PUT, "/" + INDEX_NAME + "/_doc/test", (builder, params) -> builder.field("test", "test"))
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static String verifiedSettingValue() throws IOException {
        final var settingsRequest = new Request(HttpMethod.GET.name(), "/" + INDEX_NAME + "/_settings?flat_settings");
        final Map<String, Object> settingsResponse = entityAsMap(client().performRequest(settingsRequest));
        return (String) ((Map<String, Object>) ((Map<String, Object>) settingsResponse.get(INDEX_NAME)).get("settings")).get(
            MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey()
        );
    }
}
