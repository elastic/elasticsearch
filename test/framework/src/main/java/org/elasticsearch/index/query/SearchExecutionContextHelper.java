/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.Collections;

public class SearchExecutionContextHelper {
    /**
     * Factory method generating a dummy context with partial functionality.
     */
    public static SearchExecutionContext createSimple(
        IndexSettings indexSettings,
        XContentParserConfiguration parserConfiguration,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            ClusterSettings.createBuiltInClusterSettings(),
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfiguration,
            namedWriteableRegistry,
            null,
            null,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap()
        );
    }
}
