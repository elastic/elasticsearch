/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

    /**
     * Support lookup index mode
     */
    private static final String LOOKUP_INDEX_MODE_CAPABILITY = "lookup_index_mode";

    private static final String NESTED_DENSE_VECTOR_SYNTHETIC_TEST = "nested_dense_vector_synthetic_test";

    private static final String POORLY_FORMATTED_BAD_REQUEST = "poorly_formatted_bad_request";

    private static final String HUNSPELL_DICT_400 = "hunspell_dict_400";

    public static final Set<String> CAPABILITIES = Set.of(
        LOGSDB_INDEX_MODE_CAPABILITY,
        LOOKUP_INDEX_MODE_CAPABILITY,
        NESTED_DENSE_VECTOR_SYNTHETIC_TEST,
        POORLY_FORMATTED_BAD_REQUEST,
        HUNSPELL_DICT_400
    );
}
