/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Simple in-memory based TransformConfigManager
 *
 *  NOTE: This is an incomplete implementation, only to be used for testing!
 */
public class InMemoryTransformConfigManager implements TransformConfigManager {

    public static String CURRENT_INDEX = "index-1";
    public static String OLD_INDEX = "index-0";

    private final Map<String, List<TransformCheckpoint>> checkpoints = new HashMap<>();
    private final Map<String, TransformConfig> configs = new HashMap<>();
    private final Map<String, TransformStoredDoc> transformStoredDocs = new HashMap<>();

    // for mocking updates
    private final Map<String, List<TransformCheckpoint>> oldCheckpoints = new HashMap<>();
    private final Map<String, TransformConfig> oldConfigs = new HashMap<>();
    private final Map<String, TransformStoredDoc> oldTransformStoredDocs = new HashMap<>();

    public InMemoryTransformConfigManager() {}

    public void putOldTransformCheckpoint(TransformCheckpoint checkpoint, ActionListener<Boolean> listener) {
        oldCheckpoints.compute(checkpoint.getTransformId(), (id, listOfCheckpoints) -> {
            if (listOfCheckpoints == null) {
                listOfCheckpoints = new ArrayList<TransformCheckpoint>();
            }
            listOfCheckpoints.add(checkpoint);
            return listOfCheckpoints;
        });

        listener.onResponse(true);
    }

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

    public void putOldTransformConfiguration(TransformConfig transformConfig, ActionListener<Boolean> listener) {
        oldConfigs.put(transformConfig.getId(), transformConfig);
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
        oldConfigs.remove(transformId);
        listener.onResponse(true);
    }

    @Override
    public void deleteOldTransformStoredDocuments(String transformId, ActionListener<Long> listener) {
        long deletedDocs = oldTransformStoredDocs.remove(transformId) == null ? 0L : 1L;
        listener.onResponse(deletedDocs);
    }

    @Override
    public void deleteOldCheckpoints(String transformId, long deleteCheckpointsBelow, long deleteOlderThan, ActionListener<Long> listener) {
        long deletedDocs = 0;

        final List<TransformCheckpoint> checkpointsById = checkpoints.get(transformId);
        if (checkpointsById != null) {
            final int sizeBeforeDelete = checkpointsById.size();
            checkpointsById.removeIf(cp -> { return cp.getCheckpoint() < deleteCheckpointsBelow && cp.getTimestamp() < deleteOlderThan; });
            deletedDocs += sizeBeforeDelete - checkpointsById.size();
        }

        // old checkpoints
        final List<TransformCheckpoint> checkpointsByIdOld = oldCheckpoints.get(transformId);
        if (checkpointsByIdOld != null) {
            final int sizeBeforeDeleteOldCheckpoints = checkpointsByIdOld.size();
            checkpointsByIdOld.removeIf(cp -> cp.getCheckpoint() < deleteCheckpointsBelow && cp.getTimestamp() < deleteOlderThan);
            deletedDocs += sizeBeforeDeleteOldCheckpoints - checkpointsByIdOld.size();
        }

        listener.onResponse(deletedDocs);
    }

    @Override
    public void deleteOldIndices(ActionListener<Boolean> listener) {
        if (oldCheckpoints.isEmpty() && oldConfigs.isEmpty() && oldTransformStoredDocs.isEmpty()) {
            listener.onResponse(true);
            return;
        }

        // found old documents, emulate index deletion
        oldCheckpoints.clear();
        oldConfigs.clear();
        oldTransformStoredDocs.clear();

        listener.onResponse(true);
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

        checkpointsById = oldCheckpoints.get(transformId);

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
    public void getTransformCheckpointForUpdate(
        String transformId,
        long checkpoint,
        ActionListener<Tuple<TransformCheckpoint, SeqNoPrimaryTermAndIndex>> checkpointAndVersionListener
    ) {
        List<TransformCheckpoint> checkpointsById = checkpoints.get(transformId);
        if (checkpointsById != null) {
            for (TransformCheckpoint t : checkpointsById) {
                if (t.getCheckpoint() == checkpoint) {
                    checkpointAndVersionListener.onResponse(Tuple.tuple(t, new SeqNoPrimaryTermAndIndex(1L, 1L, CURRENT_INDEX)));
                    return;
                }
            }
        }

        checkpointsById = oldCheckpoints.get(transformId);
        if (checkpointsById != null) {
            for (TransformCheckpoint t : checkpointsById) {
                if (t.getCheckpoint() == checkpoint) {
                    checkpointAndVersionListener.onResponse(Tuple.tuple(t, new SeqNoPrimaryTermAndIndex(1L, 1L, OLD_INDEX)));
                    return;
                }
            }
        }

        checkpointAndVersionListener.onResponse(null);
    }

    @Override
    public void getTransformConfiguration(String transformId, ActionListener<TransformConfig> resultListener) {
        TransformConfig config = configs.get(transformId);
        if (config == null) {
            config = oldConfigs.get(transformId);
        }

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
        TransformConfig oldConfig = oldConfigs.get(transformId);

        if (config == null && oldConfig == null) {
            configAndVersionListener.onFailure(
                new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, transformId))
            );
            return;
        }

        if (config != null) {
            configAndVersionListener.onResponse(Tuple.tuple(config, new SeqNoPrimaryTermAndIndex(1L, 1L, CURRENT_INDEX)));
        } else {
            configAndVersionListener.onResponse(Tuple.tuple(oldConfig, new SeqNoPrimaryTermAndIndex(1L, 1L, OLD_INDEX)));
        }
    }

    @Override
    public void expandTransformIds(
        String transformIdsExpression,
        PageParams pageParams,
        TimeValue timeout,
        boolean allowNoMatch,
        ActionListener<Tuple<Long, Tuple<List<String>, List<TransformConfig>>>> foundConfigsListener
    ) {
        if (Regex.isMatchAllPattern(transformIdsExpression)) {
            List<String> ids = new ArrayList<>();
            List<TransformConfig> configsAsList = new ArrayList<>();
            configs.entrySet().forEach(entry -> {
                ids.add(entry.getKey());
                configsAsList.add(entry.getValue());
            });

            oldConfigs.entrySet().forEach(entry -> {
                if (configs.containsKey(entry.getKey()) == false) {
                    ids.add(entry.getKey());
                    configsAsList.add(entry.getValue());
                }
            });

            foundConfigsListener.onResponse(new Tuple<>((long) ids.size(), Tuple.tuple(ids, configsAsList)));
            return;
        }

        if (Regex.isSimpleMatchPattern(transformIdsExpression) == false) {
            if (configs.containsKey(transformIdsExpression)) {
                foundConfigsListener.onResponse(
                    new Tuple<>(1L, Tuple.tuple(singletonList(transformIdsExpression), singletonList(configs.get(transformIdsExpression))))
                );
            } else {
                foundConfigsListener.onResponse(new Tuple<>(0L, Tuple.tuple(emptyList(), emptyList())));
            }
            return;
        }

        List<String> ids = new ArrayList<>();
        List<TransformConfig> configsAsList = new ArrayList<>();
        configs.entrySet().forEach(entry -> {
            if (Regex.simpleMatch(transformIdsExpression, entry.getKey())) {
                ids.add(entry.getKey());
                configsAsList.add(entry.getValue());
            }
        });

        oldConfigs.entrySet().forEach(entry -> {
            if (configs.containsKey(entry.getKey()) == false && Regex.simpleMatch(transformIdsExpression, entry.getKey())) {
                ids.add(entry.getKey());
                configsAsList.add(entry.getValue());
            }
        });
        foundConfigsListener.onResponse(new Tuple<>((long) ids.size(), Tuple.tuple(ids, configsAsList)));
    }

    @Override
    public void resetTransform(String transformId, ActionListener<Boolean> listener) {
        transformStoredDocs.remove(transformId);
        oldTransformStoredDocs.remove(transformId);
        checkpoints.remove(transformId);
        oldCheckpoints.remove(transformId);
    }

    @Override
    public void deleteTransform(String transformId, ActionListener<Boolean> listener) {
        configs.remove(transformId);
        oldConfigs.remove(transformId);
        resetTransform(transformId, listener);
    }

    public void putOrUpdateOldTransformStoredDoc(
        TransformStoredDoc storedDoc,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<SeqNoPrimaryTermAndIndex> listener
    ) {
        // for now we ignore seqNoPrimaryTermAndIndex
        oldTransformStoredDocs.put(storedDoc.getId(), storedDoc);
        listener.onResponse(new SeqNoPrimaryTermAndIndex(1L, 1L, OLD_INDEX));
    }

    @Override
    public void putOrUpdateTransformStoredDoc(
        TransformStoredDoc storedDoc,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ActionListener<SeqNoPrimaryTermAndIndex> listener
    ) {
        // for now we ignore seqNoPrimaryTermAndIndex
        transformStoredDocs.put(storedDoc.getId(), storedDoc);
        listener.onResponse(new SeqNoPrimaryTermAndIndex(1L, 1L, CURRENT_INDEX));
    }

    @Override
    public void getTransformStoredDoc(
        String transformId,
        boolean allowNoMatch,
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> resultListener
    ) {
        TransformStoredDoc storedDoc = transformStoredDocs.get(transformId);
        if (storedDoc != null) {
            resultListener.onResponse(Tuple.tuple(storedDoc, new SeqNoPrimaryTermAndIndex(1L, 1L, CURRENT_INDEX)));
            return;
        }

        storedDoc = oldTransformStoredDocs.get(transformId);
        if (storedDoc != null) {
            resultListener.onResponse(Tuple.tuple(storedDoc, new SeqNoPrimaryTermAndIndex(1L, 1L, OLD_INDEX)));
            return;
        }

        if (allowNoMatch) {
            resultListener.onResponse(null);
            return;
        }

        resultListener.onFailure(
            new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.UNKNOWN_TRANSFORM_STATS, transformId))
        );
    }

    @Override
    public void getTransformStoredDocs(
        Collection<String> transformIds,
        TimeValue timeout,
        ActionListener<List<TransformStoredDoc>> listener
    ) {
        List<TransformStoredDoc> docs = new ArrayList<>();
        for (String transformId : transformIds) {
            TransformStoredDoc storedDoc = transformStoredDocs.get(transformId);
            if (storedDoc != null) {
                docs.add(storedDoc);
            } else {
                storedDoc = oldTransformStoredDocs.get(transformId);
                if (storedDoc != null) {
                    docs.add(storedDoc);
                }
            }
        }
        listener.onResponse(docs);
    }

    @Override
    public void refresh(ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public void getAllTransformIds(TimeValue timeout, ActionListener<Set<String>> listener) {
        Set<String> allIds = new HashSet<>(configs.keySet());
        allIds.addAll(oldConfigs.keySet());
        listener.onResponse(allIds);
    }

    @Override
    public void getAllOutdatedTransformIds(TimeValue timeout, ActionListener<Tuple<Long, Set<String>>> listener) {
        Set<String> outdatedIds = new HashSet<>(oldConfigs.keySet());
        outdatedIds.removeAll(configs.keySet());
        listener.onResponse(new Tuple<>(Long.valueOf(configs.size() + outdatedIds.size()), outdatedIds));
    }
}
