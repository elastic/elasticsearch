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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An {@link IndexSettingProvider} is a provider for index level settings and custom index metadata that can be set
 * explicitly as a default value (so they show up as "set" for newly created indices)
 */
public interface IndexSettingProvider {
    /**
     * Allows to provide default index {@link Settings} and custom index metadata for a newly created index.
     *
     * @param indexName                             The name of the new index being created
     * @param dataStreamName                        The name of the data stream if the index being created is part of a data stream
     *                                              otherwise <code>null</code>
     * @param templateIndexMode                     The index mode defined in template if template creates data streams,
     *                                              otherwise <code>null</code> is returned.
     * @param projectMetadata                       The current project metadata instance that doesn't yet contain the index to be created
     * @param resolvedAt                            The time the request to create this new index was accepted.
     * @param indexTemplateAndCreateRequestSettings All the settings resolved from the template that matches and any settings
     *                                              defined on the create index request
     * @param combinedTemplateMappings              All the mappings resolved from the template that matches
     * @param additionalSettings                    A settings builder to which additional settings can be added
     * @param additionalCustomMetadata              A consumer to which additional
     *                                              {@linkplain IndexMetadata.Builder#putCustom(String, Map) custom index metadata}
     *                                              can be added
     */
    void provideAdditionalMetadata(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        Settings.Builder additionalSettings,
        BiConsumer<String, Map<String, String>> additionalCustomMetadata
    );

    /**
     * Called when the mappings for an existing index are updated, before the new index metadata is created.
     * This method can be used to update index settings and to provide custom metadata based on the new mappings.
     *
     * @param indexMetadata            The index metadata for the index being updated
     * @param documentMapper           The document mapper containing the updated mappings
     * @param additionalSettings       A settings builder to which additional settings can be added
     * @param additionalCustomMetadata A consumer to which additional
     *                                 {@linkplain IndexMetadata.Builder#putCustom(String, Map) custom index metadata}
     *                                 can be added
     */
    default void onUpdateMappings(
        IndexMetadata indexMetadata,
        DocumentMapper documentMapper,
        Settings.Builder additionalSettings,
        BiConsumer<String, Map<String, String>> additionalCustomMetadata
    ) {}

    /**
     * Infrastructure class that holds services that can be used by {@link IndexSettingProvider} instances.
     */
    record Parameters(ClusterService clusterService, CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory) {

    }

    /**
     * Indicates whether the additional settings that this provider returns can overrule the settings defined in matching template
     * or in create index request.
     *
     * Note that this is not used during index template validation, to avoid overruling template settings that may apply to
     * different contexts (e.g. the provider is not used, or it returns different setting values).
     */
    default boolean overrulesTemplateAndRequestSettings() {
        return false;
    }
}
