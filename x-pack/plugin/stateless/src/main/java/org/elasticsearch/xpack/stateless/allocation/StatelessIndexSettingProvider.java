/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class StatelessIndexSettingProvider implements IndexSettingProvider {

    public StatelessIndexSettingProvider() {}

    @Override
    public void provideAdditionalSettings(
        String indexName,
        @Nullable String dataStreamName,
        IndexMode templateIndexMode,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        IndexVersion indexVersion,
        Settings.Builder additionalSettings
    ) {
        // TODO find a prover way to bypass index template validation
        if (Objects.equals(indexName, "validate-index-name") == false) {
            additionalSettings.put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), StatelessPlugin.NAME);
        }
    }
}
