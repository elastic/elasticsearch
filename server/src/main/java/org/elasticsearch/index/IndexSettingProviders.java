/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps track of the {@link IndexSettingProvider} instances defined by plugins and
 * this class can be used by other components to get access to {@link IndexSettingProvider} instances.
 */
public final class IndexSettingProviders {

    private final Set<IndexSettingProvider> indexSettingProviders;

    public IndexSettingProviders(Set<IndexSettingProvider> indexSettingProviders) {
        this.indexSettingProviders = Collections.unmodifiableSet(indexSettingProviders);
    }

    public Set<IndexSettingProvider> getIndexSettingProviders() {
        return indexSettingProviders;
    }

    /**
     * Runs all the given providers and collects the settings they contribute. Every component that needs to know which settings an
     * index will end up with should go through this method, so they all agree with what {@link MetadataCreateIndexService} does when
     * the index is actually created.
     *
     * @throws IllegalArgumentException if two providers contribute the same setting, or a provider sets the index created version
     */
    public static AdditionalSettings collectAdditionalSettings(
        Collection<IndexSettingProvider> providers,
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        Metadata metadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        Settings.Builder additionalSettings = Settings.builder();
        Set<String> overrulingSettings = new HashSet<>();
        for (IndexSettingProvider provider : providers) {
            Settings providedSettings = provider.getAdditionalIndexSettings(
                indexName,
                dataStreamName,
                templateIndexMode,
                metadata,
                resolvedAt,
                indexTemplateAndCreateRequestSettings,
                combinedTemplateMappings
            );
            MetadataCreateIndexService.validateAdditionalSettings(provider, providedSettings, additionalSettings);
            additionalSettings.put(providedSettings);
            if (provider.overrulesTemplateAndRequestSettings()) {
                overrulingSettings.addAll(providedSettings.keySet());
            }
        }
        return new AdditionalSettings(additionalSettings.build(), Collections.unmodifiableSet(overrulingSettings));
    }

    /**
     * The settings contributed by the {@link IndexSettingProvider}s for a single index.
     *
     * @param settings the settings contributed by all providers
     * @param overrulingSettings the keys contributed by providers that {@link IndexSettingProvider#overrulesTemplateAndRequestSettings()
     *                           overrule} the template and request settings
     */
    public record AdditionalSettings(Settings settings, Set<String> overrulingSettings) {

        /**
         * Applies the provided settings as defaults to the given settings: a value in {@code userDefinedSettings} wins over a provided one,
         * unless the provider overrules it. An explicit {@code null} is removed.
         * @param userDefinedSettings the resolved settings from user input
         * @return the effective settings
         */
        public Settings applyTo(Settings userDefinedSettings) {
            Settings.Builder builder = Settings.builder();
            applyTo(userDefinedSettings, builder, null);
            return builder.build();
        }

        /**
         * Applies the provided settings as defaults to the given settings: a value in {@code userDefinedSettings} wins over a provided one,
         * unless the provider overrules it. An explicit {@code null} in {@code userDefinedSettings} is removed unless it is part of the
         * {@code requestSettingsBuilder}, overruled settings are removed from the {@code requestSettingsBuilder}.
         * @param userDefinedSettings the resolved settings from user input
         * @param resultBuilder the builder with the effective settings after the user defined settings are merged with
         *                                 the additional ones
         * @param requestSettingsBuilder the builder with the explicit request settings that need to be cleared from the
         *                               overruling settings.
         */
        public void applyTo(
            Settings userDefinedSettings,
            Settings.Builder resultBuilder,
            @Nullable Settings.Builder requestSettingsBuilder
        ) {
            Set<String> userDefinedSettingNames = userDefinedSettings.keySet();
            Set<String> additionalSettingNames = settings.keySet();
            Set<String> preserveNulls = requestSettingsBuilder == null ? Set.of() : requestSettingsBuilder.keys();
            for (String additionalSetting : additionalSettingNames) {
                if (additionalSettingNames.contains(additionalSetting) == false) {
                    continue;
                }
                boolean nonNullValue = settings.get(additionalSetting) != null;
                if (overrulingSettings.contains(additionalSetting)) {
                    if (nonNullValue) {
                        resultBuilder.copy(additionalSetting, settings);
                    }
                    if (requestSettingsBuilder != null) {
                        requestSettingsBuilder.remove(additionalSetting);
                    }
                } else if (userDefinedSettingNames.contains(additionalSetting) == false && nonNullValue) {
                    resultBuilder.copy(additionalSetting, settings);
                }
            }
            for (String userDefinedSetting : userDefinedSettingNames) {
                // The providers have already provided an overruling value, so this has the correct value already
                if (overrulingSettings.contains(userDefinedSetting) && additionalSettingNames.contains(userDefinedSetting)) {
                    continue;
                }
                if (userDefinedSettings.get(userDefinedSetting) != null || preserveNulls.contains(userDefinedSetting)) {
                    resultBuilder.copy(userDefinedSetting, userDefinedSettings);
                }
            }
        }
    }
}
