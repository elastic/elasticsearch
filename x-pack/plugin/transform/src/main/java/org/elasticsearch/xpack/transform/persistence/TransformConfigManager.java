/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface TransformConfigManager {

    Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(TransformField.FOR_INTERNAL_STORAGE, "true");

    /**
     * Persist a checkpoint in the internal index
     *
     * @param checkpoint the @link{TransformCheckpoint}
     * @param listener listener to call after request has been made
     */
    void putTransformCheckpoint(TransformCheckpoint checkpoint, ActionListener<Boolean> listener);

    /**
     * Store the transform configuration in the internal index
     *
     * @param transformConfig the @link{TransformConfig}
     * @param listener listener to call after request
     */
    void putTransformConfiguration(TransformConfig transformConfig, ActionListener<Boolean> listener);

    /**
     * Update the transform configuration in the internal index.
     *
     * Essentially the same as {@link IndexBasedTransformConfigManager#putTransformConfiguration(TransformConfig, ActionListener)}
     * but is an index operation that will fail with a version conflict
     * if the current document seqNo and primaryTerm is not the same as the provided version.
     * @param transformConfig the @link{TransformConfig}
     * @param seqNoPrimaryTermAndIndex an object containing the believed seqNo, primaryTerm and index for the doc.
     *                             Used for optimistic concurrency control
     * @param listener listener to call after request
     */
    void updateTransformConfiguration(
        TransformConfig transformConfig,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<Boolean> listener
    );

    /**
     * This deletes configuration documents that match the given transformId that are contained in old index versions.
     *
     * @param transformId The configuration ID potentially referencing configurations stored in the old indices
     * @param listener listener to alert on completion
     */
    void deleteOldTransformConfigurations(String transformId, ActionListener<Boolean> listener);

    /**
     * This deletes stored state/stats documents for the given transformId that are contained in old index versions.
     *
     * @param transformId The transform ID referenced by the documents
     * @param listener listener to alert on completion, returning the number of deleted docs
     */
    void deleteOldTransformStoredDocuments(String transformId, ActionListener<Long> listener);

    /**
     * This deletes stored checkpoint documents for the given transformId, based on number and age.
     *
     * Both criteria MUST apply for the deletion to happen.
     *
     * @param transformId The transform ID referenced by the documents
     * @param deleteCheckpointsBelow checkpoints lower than this to delete
     * @param deleteOlderThan checkpoints older than this to delete
     * @param listener listener to alert on completion, returning number of deleted checkpoints
     */
    void deleteOldCheckpoints(String transformId, long deleteCheckpointsBelow, long deleteOlderThan, ActionListener<Long> listener);

    /**
     * Get a stored checkpoint, requires the transform id as well as the checkpoint id
     *
     * @param transformId the transform id
     * @param checkpoint the checkpoint
     * @param resultListener listener to call after request has been made
     */
    void getTransformCheckpoint(String transformId, long checkpoint, ActionListener<TransformCheckpoint> resultListener);

    /**
     * Get the transform configuration for a given transform id. This function is only for internal use. For transforms returned via GET
     * _transform, see the @link{TransportGetTransformAction}
     *
     * @param transformId the transform id
     * @param resultListener listener to call after inner request has returned
     */
    void getTransformConfiguration(String transformId, ActionListener<TransformConfig> resultListener);

    /**
     * Get the transform configuration for a given transform id. This function is only for internal use. For transforms returned via GET
     * _transform, see the @link{TransportGetTransformAction}
     *
     * @param transformId the transform id
     * @param configAndVersionListener listener to call after inner request has returned
     */
    void getTransformConfigurationForUpdate(
        String transformId,
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> configAndVersionListener
    );

    /**
     * Given some expression comma delimited string of id expressions,
     *   this queries our internal index for the transform Ids that match the expression.
     *
     * The results are sorted in ascending order
     *
     * @param transformIdsExpression The id expression. Can be _all, *, or comma delimited list of simple regex strings
     * @param pageParams             The paging params
     * @param foundConfigsListener   The listener on signal on success or failure
     */
    void expandTransformIds(
        String transformIdsExpression,
        PageParams pageParams,
        boolean allowNoMatch,
        ActionListener<Tuple<Long, Tuple<List<String>, List<TransformConfig>>>> foundConfigsListener
    );

    /**
     * This deletes the configuration and all other documents corresponding to the transform id (e.g. checkpoints).
     *
     * @param transformId the transform id
     * @param listener listener to call after inner request returned
     */
    void deleteTransform(String transformId, ActionListener<Boolean> listener);

    void putOrUpdateTransformStoredDoc(
        TransformStoredDoc storedDoc,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<SeqNoPrimaryTermAndIndex> listener
    );

    void getTransformStoredDoc(String transformId, ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> resultListener);

    void getTransformStoredDocs(Collection<String> transformIds, ActionListener<List<TransformStoredDoc>> listener);

    void refresh(ActionListener<Boolean> listener);
}
