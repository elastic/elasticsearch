/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Keeps track of the {@link IndexSettingProvider} instances defined by plugins and
 * this class can be used by other components to get access to {@link IndexSettingProvider} instances.
 */
public final class IndexSettingProviders {

    public static IndexSettingProviders EMPTY = new IndexSettingProviders(Collections.emptySet());

    private final Set<IndexSettingProvider> indexSettingProviders;

    /**
     * Utility method which creates an {@link IndexSettingProviders} instance that uses the provided consumer to add settings
     * to the index being created.
     * The primary use case is for tests that want to add specific settings without having to create a full implementation.
     *
     * @param settingsBuilderConsumer A consumer that adds index settings
     * @return An {@link IndexSettingProviders} instance that uses the provided consumer to add settings
     */
    public static IndexSettingProviders of(Consumer<Settings.Builder> settingsBuilderConsumer) {
        return new IndexSettingProviders(
            Set.of(
                (
                    indexName,
                    dataStreamName,
                    templateIndexMode,
                    projectMetadata,
                    resolvedAt,
                    indexTemplateAndCreateRequestSettings,
                    combinedTemplateMappings,
                    indexVersion,
                    additionalSettings) -> settingsBuilderConsumer.accept(additionalSettings)
            )
        );
    }

    public IndexSettingProviders(Set<IndexSettingProvider> indexSettingProviders) {
        this.indexSettingProviders = Collections.unmodifiableSet(indexSettingProviders);
    }

    public Set<IndexSettingProvider> getIndexSettingProviders() {
        return indexSettingProviders;
    }
}
