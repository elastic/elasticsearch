/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple in-memory based TransformConfigManager
 *
 *  NOTE: This is an incomplete implementation, only to be used for testing!
 */
public class InMemoryTransformConfigManager implements TransformConfigManager {

    private final Map<String, List<TransformCheckpoint>> checkpoints = new HashMap<>();
    private final Map<String, TransformConfig> configs = new HashMap<>();
    private final Map<String, TransformStoredDoc> transformStoredDocs = new HashMap<>();

    public InMemoryTransformConfigManager() {}

    @Override
    public void putTransformCheckpoint(TransformCheckpoint checkpoint, ActionListener<Boolean> listener) {
        checkpoints.compute(checkpoint.getTransformId(), (id, listOfCheckpoints) -> {
            if (listOfCheckpoints == null) {
                listOfCheckpoints = new ArrayList<TransformCheckpoint>();
            }
            listOfCheckpoints.add(checkpoint);
            return listOfCheckpoints;
        });

        listener.onResponse(true);
    }

    @Override
    public void putTransformConfiguration(TransformConfig transformConfig, ActionListener<Boolean> listener) {
        configs.put(transformConfig.getId(), transformConfig);
        listener.onResponse(true);
    }

    @Override
    public void updateTransformConfiguration(
        TransformConfig transformConfig,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<Boolean> listener
    ) {

        // for now we ignore seqNoPrimaryTermAndIndex
        configs.put(transformConfig.getId(), transformConfig);
        listener.onResponse(true);
    }

    @Override
    public void deleteOldTransformConfigurations(String transformId, ActionListener<Boolean> listener) {
        configs.remove(transformId);
        listener.onResponse(true);
    }

    @Override
    public void deleteOldTransformStoredDocuments(String transformId, ActionListener<Boolean> listener) {
        transformStoredDocs.remove(transformId);
        listener.onResponse(true);
    }

    @Override
    public void deleteOldCheckpoints(String transformId, long deleteCheckpointsBelow, long deleteOlderThan, ActionListener<Long> listener) {
        List<TransformCheckpoint> checkpointsById = checkpoints.get(transformId);
        int sizeBeforeDelete = checkpointsById.size();
        if (checkpointsById != null) {
            checkpointsById.removeIf(cp -> { return cp.getCheckpoint() < deleteCheckpointsBelow && cp.getTimestamp() < deleteOlderThan; });
        }
        listener.onResponse(Long.valueOf(sizeBeforeDelete - checkpointsById.size()));
    }

    @Override
    public void getTransformCheckpoint(String transformId, long checkpoint, ActionListener<TransformCheckpoint> resultListener) {
        List<TransformCheckpoint> checkpointsById = checkpoints.get(transformId);

        if (checkpointsById != null) {
            for (TransformCheckpoint t : checkpointsById) {
                if (t.getCheckpoint() == checkpoint) {
                    resultListener.onResponse(t);
                    return;
                }
            }
        }

        resultListener.onResponse(TransformCheckpoint.EMPTY);
    }

    @Override
    public void getTransformConfiguration(String transformId, ActionListener<TransformConfig> resultListener) {
        TransformConfig config = configs.get(transformId);
        if (config == null) {
            resultListener.onFailure(
                new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
            );
            return;
        }
        resultListener.onResponse(config);
    }

    @Override
    public void getTransformConfigurationForUpdate(
        String transformId,
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> configAndVersionListener
    ) {
        TransformConfig config = configs.get(transformId);
        if (config == null) {
            configAndVersionListener.onFailure(
                new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
            );
            return;
        }

        configAndVersionListener.onResponse(Tuple.tuple(config, new SeqNoPrimaryTermAndIndex(1L, 1L, "index-1")));
    }

    @Override
    public void expandTransformIds(
        String transformIdsExpression,
        PageParams pageParams,
        boolean allowNoMatch,
        ActionListener<Tuple<Long, List<String>>> foundIdsListener
    ) {

        if (Regex.isMatchAllPattern(transformIdsExpression)) {
            List<String> ids = new ArrayList<>(configs.keySet());
            foundIdsListener.onResponse(new Tuple<>((long) ids.size(), ids));
            return;
        }

        if (!Regex.isSimpleMatchPattern(transformIdsExpression)) {
            if (configs.containsKey(transformIdsExpression)) {
                foundIdsListener.onResponse(new Tuple<>(1L, Collections.singletonList(transformIdsExpression)));
            } else {
                foundIdsListener.onResponse(new Tuple<>(0L, Collections.emptyList()));
            }
            return;
        }
        Set<String> ids = new LinkedHashSet<>();
        configs.keySet().forEach(id -> {
            if (Regex.simpleMatch(transformIdsExpression, id)) {
                ids.add(id);
            }
        });
        foundIdsListener.onResponse(new Tuple<>((long) ids.size(), new ArrayList<>(ids)));
    }

    @Override
    public void deleteTransform(String transformId, ActionListener<Boolean> listener) {
        configs.remove(transformId);
        transformStoredDocs.remove(transformId);
        checkpoints.remove(transformId);
    }

    @Override
    public void putOrUpdateTransformStoredDoc(
        TransformStoredDoc storedDoc,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<SeqNoPrimaryTermAndIndex> listener
    ) {

        // for now we ignore seqNoPrimaryTermAndIndex
        transformStoredDocs.put(storedDoc.getId(), storedDoc);
        listener.onResponse(new SeqNoPrimaryTermAndIndex(1L, 1L, "index-1"));
    }

    @Override
    public void getTransformStoredDoc(
        String transformId,
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> resultListener
    ) {

        TransformStoredDoc storedDoc = transformStoredDocs.get(transformId);
        if (storedDoc == null) {
            resultListener.onFailure(
                new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.UNKNOWN_TRANSFORM_STATS, transformId))
            );
            return;
        }

        resultListener.onResponse(Tuple.tuple(storedDoc, new SeqNoPrimaryTermAndIndex(1L, 1L, "index-1")));
    }

    @Override
    public void getTransformStoredDocs(Collection<String> transformIds, ActionListener<List<TransformStoredDoc>> listener) {
        List<TransformStoredDoc> docs = new ArrayList<>();
        for (String transformId : transformIds) {
            TransformStoredDoc storedDoc = transformStoredDocs.get(transformId);
            if (storedDoc != null) {
                docs.add(storedDoc);
            }
        }
        listener.onResponse(docs);
    }

}
