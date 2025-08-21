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
import org.elasticsearch.common.collect.ImmutableOpenMap;
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

/**
 * An {@link IndexSettingProvider} is a provider for index level settings that can be set
 * explicitly as a default value (so they show up as "set" for newly created indices)
 */
public interface IndexSettingProvider {
    /**
     * Returns explicitly set default index {@link Settings} for the given index. This should not
     * return null.
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
     * @param extraCustomMetadata                 A builder for custom metadata that can be used to add custom index metadata
     */
    Settings getAdditionalIndexSettings(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        ImmutableOpenMap.Builder<String, Map<String, String>> extraCustomMetadata
    );

    /**
     * Called when the mappings for an index are updated.
     * This method can be used to update index settings based on the new mappings.
     *
     * @param indexMetadata        the index metadata for the index being updated
     * @param extraCustomMetadata  a builder for custom metadata that can be used to add custom index metadata
     * @param documentMapper       the document mapper containing the updated mappings
     * @return additional settings to be applied to the index
     * or {@link Settings#EMPTY}/{@code null} if no additional settings are needed
     */
    @Nullable
    default Settings onUpdateMappings(
        IndexMetadata indexMetadata,
        ImmutableOpenMap.Builder<String, Map<String, String>> extraCustomMetadata,
        DocumentMapper documentMapper
    ) {
        return Settings.EMPTY;
    }

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
