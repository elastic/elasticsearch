/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * An {@link IndexSettingProvider} is a provider for index level settings that can be set
 * explicitly as a default value (so they show up as "set" for newly created indices)
 */
public interface IndexSettingProvider {
    /**
     * Returns explicitly set default index {@link Settings} for the given index. This should not
     * return null.
     *
     * @param indexName                The name of the new index being created
     * @param dataStreamName           The name of the data stream if the index being created is part of a data stream otherwise
     *                                 <code>null</code>
     * @param timeSeries               Whether the template is in time series mode.
     * @param metadata                 The current metadata instance that doesn't yet contain the index to be created
     * @param resolvedAt               The time the request to create this new index was accepted.
     * @param allSettings              All the setting resolved from the template that matches and any setting defined on the create index
     *                                 request
     * @param combinedTemplateMappings All the mappings resolved from the template that matches
     */
    Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean timeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings
    );

    /**
     * Infrastructure class that holds services that can be used by {@link IndexSettingProvider} instances.
     */
    record Parameters(CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory) {

    }
}
