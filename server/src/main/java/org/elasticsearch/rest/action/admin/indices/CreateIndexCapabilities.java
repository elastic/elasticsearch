/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestCreateIndexAction}.
 */
public class CreateIndexCapabilities {

    /**
     * Support for using the 'logs' index mode.
     */
    private static final String LOGSDB_INDEX_MODE_CAPABILITY = "logsdb_index_mode";

    public static Set<String> CAPABILITIES = Set.of(LOGSDB_INDEX_MODE_CAPABILITY);
}
