/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.HashSet;
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

    /**
     * Support for columnar and logsdb_columnar index modes.
     */
    private static final String COLUMNAR_INDEX_MODES_CAPABILITY = "columnar_index_modes";

    /**
     * Support vectordb_document index mode
     */
    private static final String VECTORDB_DOCUMENT_INDEX_MODE_CAPABILITY = "vectordb_document_index_mode";

    private static final String NESTED_DENSE_VECTOR_SYNTHETIC_TEST = "nested_dense_vector_synthetic_test";

    private static final String POORLY_FORMATTED_BAD_REQUEST = "poorly_formatted_bad_request";

    private static final String HUNSPELL_DICT_400 = "hunspell_dict_400";

    private static final String DISABLE_SEQUENCE_NUMBERS_CAPABILITY = "disable_sequence_numbers";

    private static final String REJECT_RUNTIME_FIELD_SHADOWING_SORT_FIELD = "reject_runtime_field_shadowing_sort_field";

    /**
     * Support for slice-enabled indices ({@code index.slice.enabled}). Advertised only when the feature flag is on, so
     * yaml tests can gate on it and skip on builds where slice indexing is unavailable.
     */
    private static final String SLICE_INDEXING_CAPABILITY = "slice_indexing";

    /**
     * Support for {@code doc_values.on_failure=ignore} ({@code index.mapping.doc_values.on_failure=ignore}). Advertised
     * only when the feature flag is on, so YAML tests can gate on it and skip on builds where the feature is unavailable.
     */
    private static final String DOC_VALUES_ON_FAILURE_CAPABILITY = "doc_values_on_failure";

    public static final Set<String> CAPABILITIES;

    static {
        var caps = new HashSet<>(
            Set.of(
                LOGSDB_INDEX_MODE_CAPABILITY,
                LOOKUP_INDEX_MODE_CAPABILITY,
                NESTED_DENSE_VECTOR_SYNTHETIC_TEST,
                POORLY_FORMATTED_BAD_REQUEST,
                HUNSPELL_DICT_400,
                DISABLE_SEQUENCE_NUMBERS_CAPABILITY,
                REJECT_RUNTIME_FIELD_SHADOWING_SORT_FIELD
            )
        );
        caps.add(COLUMNAR_INDEX_MODES_CAPABILITY);
        caps.add(VECTORDB_DOCUMENT_INDEX_MODE_CAPABILITY);
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled()) {
            caps.add(SLICE_INDEXING_CAPABILITY);
        }
        if (FieldMapper.DOC_VALUES_ON_FAILURE_FEATURE_FLAG.isEnabled()) {
            caps.add(DOC_VALUES_ON_FAILURE_CAPABILITY);
        }
        CAPABILITIES = Set.copyOf(caps);
    }
}
