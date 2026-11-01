/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.List;

import static org.elasticsearch.cluster.node.DiscoveryNode.STATELESS_ENABLED_SETTING_NAME;

public class IndexWarmerTests extends ESTestCase {

    public void testShouldWarmGlobalOrdinals() {
        for (var hasIndexRole : List.of(true, false)) {
            for (var isStateless : List.of(true, false)) {
                boolean result = IndexWarmer.shouldWarmGlobalOrdinals(indexWarmerSettings(isStateless, hasIndexRole));
                if (isStateless) {
                    assertEquals(hasIndexRole == false, result);
                } else {
                    assertTrue(result);
                }
            }
        }
    }

    private IndexSettings indexWarmerSettings(boolean isStateless, boolean hasIndexRole) {
        var nodeSettingsBuilder = Settings.builder()
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                hasIndexRole ? DiscoveryNodeRole.INDEX_ROLE.roleName() : DiscoveryNodeRole.SEARCH_ROLE.roleName()
            )
            .put(STATELESS_ENABLED_SETTING_NAME, isStateless);

        return IndexSettingsModule.newIndexSettings(
            new org.elasticsearch.index.Index("index", IndexMetadata.INDEX_UUID_NA_VALUE),
            Settings.EMPTY,
            nodeSettingsBuilder.build()
        );
    }
}
