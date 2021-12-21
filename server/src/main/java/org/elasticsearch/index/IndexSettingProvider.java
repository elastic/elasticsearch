/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;

/**
 * An {@link IndexSettingProvider} is a provider for index level settings that can be set
 * explicitly as a default value (so they show up as "set" for newly created indices)
 */
public interface IndexSettingProvider {
    /**
     * Returns explicitly set default index {@link Settings} for the given index. This should not
     * return null.
     *
     * @param indexName         The name of the new index being created
     * @param dataStreamName    The name of the data stream if the index being created is part of a data stream otherwise <code>null</code>
     * @param newDataStream     Whether the data stream is also new and doesn't exist yet if this index is part of a data stream.
     * @param resolvedAt        The time the request to create this new index was accepted.
     * @param allSettings       All the setting resolved from the template that matches and any setting defined on the create index request
     */
    Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean newDataStream,
        long resolvedAt,
        Settings allSettings
    );
}
