/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Resolves whether optimistic concurrency control (OCC) should be used for bulk-by-scroll actions
 * (update-by-query, delete-by-query) based on the {@code index.disable_sequence_numbers} setting
 * across the target indices. Rejects mixed configurations unless indices belong to the same data stream.
 */
final class BulkByScrollOCCResolver {

    private BulkByScrollOCCResolver() {}

    /**
     * Resolves whether OCC (optimistic concurrency control via seq_no/primary_term) should be used
     * based on the {@link IndexSettings#DISABLE_SEQUENCE_NUMBERS} setting of the target indices.
     *
     * <p>When backing indices within a data stream have mixed settings, OCC is disabled because
     * search hits from backing indices with sequence numbers disabled will not carry valid values.
     *
     * @return {@code true} if OCC should be used, {@code false} otherwise
     * @throws ActionRequestValidationException if indices have mixed setting values across
     *                                          different data streams or standalone indices
     */
    static boolean resolveUseOCC(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectMetadata projectMetadata,
        AbstractBulkByScrollRequest<?> request
    ) {
        if (IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG == false) {
            return true;
        }
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(projectMetadata, request.getSearchRequest());
        if (concreteIndices.length == 0) {
            return true;
        }

        SortedMap<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        Map<String, Boolean> seqNoDisabledPerSource = new HashMap<>();
        for (Index concreteIndex : concreteIndices) {
            IndexMetadata indexMetadata = projectMetadata.index(concreteIndex);
            boolean seqNoDisabled = hasSequenceNumbersDisabled(indexMetadata);

            IndexAbstraction indexAbstraction = indicesLookup.get(concreteIndex.getName());
            DataStream parentDataStream = indexAbstraction != null ? indexAbstraction.getParentDataStream() : null;

            if (parentDataStream != null) {
                seqNoDisabledPerSource.merge(parentDataStream.getName(), seqNoDisabled, (existing, current) -> existing || current);
            } else {
                seqNoDisabledPerSource.put(concreteIndex.getName(), seqNoDisabled);
            }
        }

        boolean seqNoDisabled = seqNoDisabledPerSource.values().iterator().next();
        for (boolean disabled : seqNoDisabledPerSource.values()) {
            if (disabled != seqNoDisabled) {
                ActionRequestValidationException exception = new ActionRequestValidationException();
                exception.addValidationError(
                    "cannot perform bulk-by-scroll across indices with mixed values for ["
                        + IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey()
                        + "]. All target indices must have the same setting value unless they belong to the same data stream."
                );
                throw exception;
            }
        }
        return seqNoDisabled == false;
    }

    static boolean hasSequenceNumbersDisabled(IndexMetadata indexMetadata) {
        // TODO: extract this into indexMetadata
        return IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG
            && IndexSettings.DISABLE_SEQUENCE_NUMBERS.get(indexMetadata.getSettings());
    }
}
