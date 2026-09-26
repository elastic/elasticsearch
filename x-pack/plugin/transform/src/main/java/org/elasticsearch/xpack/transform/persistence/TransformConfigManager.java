/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
     * This deletes all _old_ internal storages(indices) except the most recent one.
     *
     * CAUTION: Deletes data without checks! Special method for upgrades.
     *
     * @param listener listener to call on completion
     */
    void deleteOldIndices(ActionListener<Boolean> listener);

    /**
     * Get a stored checkpoint, requires the transform id as well as the checkpoint id
     *
     * @param transformId the transform id
     * @param checkpoint the checkpoint
     * @param checkpointListener listener to call after request has been made
     */
    void getTransformCheckpoint(String transformId, long checkpoint, ActionListener<TransformCheckpoint> checkpointListener);

    /**
     * Get a stored checkpoint, requires the transform id as well as the checkpoint id. This function is only for internal use.
     *
     * @param transformId the transform id
     * @param checkpoint the checkpoint
     * @param checkpointAndVersionListener listener to call after inner request has returned
     */
    void getTransformCheckpointForUpdate(
        String transformId,
        long checkpoint,
        ActionListener<Tuple<TransformCheckpoint, SeqNoPrimaryTermAndIndex>> checkpointAndVersionListener
    );

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
     * @param timeout                The timeout applied to all the spawned requests
     * @param foundConfigsListener   The listener on signal on success or failure
     */
    void expandTransformIds(
        String transformIdsExpression,
        PageParams pageParams,
        TimeValue timeout,
        boolean allowNoMatch,
        ActionListener<Tuple<Long, Tuple<List<String>, List<TransformConfig>>>> foundConfigsListener
    );

    /**
     * Get all transform ids
     *
     * @param timeout  The timeout applied to all the spawned requests
     * @param listener The listener to call with the collected ids
     */
    void getAllTransformIds(TimeValue timeout, ActionListener<Set<String>> listener);

    /**
     * Get all transform ids that aren't using the latest index.
     *
     * @param timeout  The timeout applied to all the spawned requests
     * @param listener The listener to call with total number of transforms and the list of transform ids.
     */
    void getAllOutdatedTransformIds(TimeValue timeout, ActionListener<Tuple<Long, Set<String>>> listener);

    /**
     * This deletes documents corresponding to the transform id (e.g. checkpoints).
     * Configuration is left intact.
     *
     * @param transformId the transform id
     * @param listener listener to call after inner request returned
     */
    void resetTransform(String transformId, ActionListener<Boolean> listener);

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

    void getTransformStoredDoc(
        String transformId,
        boolean allowNoMatch,
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> resultListener
    );

    void getTransformStoredDocs(Collection<String> transformIds, TimeValue timeout, ActionListener<List<TransformStoredDoc>> listener);

    void refresh(ActionListener<Boolean> listener);

    String CLOUD_CREDENTIAL_DOC_TYPE = "data_frame_transform_cloud_credential";
    String CLOUD_CREDENTIAL_TOKEN_ID_FIELD = "token_id";
    String CLOUD_CREDENTIAL_TRANSFORM_ID_FIELD = "transform_id";

    /**
     * The storage document id for a cloud credential. Cloud credentials are keyed by their UIAM
     * {@code tokenId}, not the owning {@code transformId}: minting a new credential during update
     * creates a new document so the prior credential remains intact until the indexer's {@code onStart}
     * (running task) or the update action (stopped task) explicitly revokes and deletes it.
     */
    static String cloudCredentialDocumentId(String tokenId) {
        return CLOUD_CREDENTIAL_DOC_TYPE + "-" + tokenId;
    }

    /**
     * Persist a cloud credential. The credential is written under id {@code cloudCredentialDocumentId(credential.id())}
     * using {@code op_type=create} so a duplicate {@code tokenId} fails fast with a version conflict
     * (callers can then surface a transactional error rather than silently overwriting). The body
     * additionally carries {@code token_id} and {@code transform_id} fields so future sweeps can
     * reconcile orphans by transform.
     *
     * @param transformId the owning transform id (recorded in the document body)
     * @param credential  the credential envelope to persist (its {@code id()} is the storage key)
     * @param listener    listener to call after request
     */
    void putTransformCloudCredential(String transformId, PersistedCloudCredential credential, ActionListener<Boolean> listener);

    /**
     * Load a previously persisted cloud credential by its UIAM {@code tokenId}.
     *
     * @param tokenId      the UIAM token id (the credential's {@code id()})
     * @param allowNoMatch if true, return null when no credential exists; otherwise fail with ResourceNotFoundException
     * @param listener     listener to call with the credential or null
     */
    void getTransformCloudCredentialByTokenId(String tokenId, boolean allowNoMatch, ActionListener<PersistedCloudCredential> listener);

    /**
     * Delete a persisted cloud credential by its UIAM {@code tokenId}.
     *
     * @param tokenId  the UIAM token id (the credential's {@code id()})
     * @param listener listener to call after request (true if deleted, false if not found)
     */
    void deleteCloudCredentialByTokenId(String tokenId, ActionListener<Boolean> listener);

    /**
     * Calls {@code action} for every cloud credential stored for the given {@code transformId},
     * including both the currently-active credential and any dangling credentials left by
     * interrupted rotations. Paginates internally so all credentials are visited regardless of
     * count. Calls {@code listener} with {@code null} once all credentials have been processed,
     * or on failure if a page fetch fails.
     *
     * @param transformId the transform id whose credentials should be visited
     * @param action      called once per credential; must not throw
     * @param listener    called when iteration is complete or on page-fetch failure
     */
    void forEachTransformCloudCredential(String transformId, Consumer<PersistedCloudCredential> action, ActionListener<Void> listener);

    default boolean isLatestTransformIndex(String indexName) {
        return TransformInternalIndexConstants.LATEST_INDEX_NAME.equals(indexName);
    }
}
