/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.tasks.TaskId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexUpgradeService {

    private static final Logger logger = LogManager.getLogger(IndexUpgradeService.class);

    private final List<IndexUpgradeCheck> upgradeChecks;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexUpgradeService(List<IndexUpgradeCheck> upgradeChecks) {
        this.upgradeChecks = upgradeChecks;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver();
    }

    /**
     * Returns the information about required upgrade action for the given indices
     *
     * @param indices list of indices to check, specify _all for all indices
     * @param options wild card resolution option
     * @param state   the current cluster state
     * @return a list of indices that should be upgraded/reindexed
     */
    public Map<String, UpgradeActionRequired> upgradeInfo(String[] indices, IndicesOptions options, ClusterState state) {
        Map<String, UpgradeActionRequired> results = new HashMap<>();
        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, options, indices);
        MetaData metaData = state.getMetaData();
        for (String index : concreteIndexNames) {
            IndexMetaData indexMetaData = metaData.index(index);
            UpgradeActionRequired upgradeActionRequired = upgradeInfo(indexMetaData, index);
            if (upgradeActionRequired != null) {
                results.put(index, upgradeActionRequired);
            }
        }
        return results;
    }

    private UpgradeActionRequired upgradeInfo(IndexMetaData indexMetaData, String index) {
        for (IndexUpgradeCheck check : upgradeChecks) {
            UpgradeActionRequired upgradeActionRequired = check.actionRequired(indexMetaData);
            logger.trace("[{}] check [{}] returned [{}]", index, check.getName(), upgradeActionRequired);
            switch (upgradeActionRequired) {
                case UPGRADE:
                case REINDEX:
                    // this index needs to be upgraded or reindexed - skipping all other checks
                    return upgradeActionRequired;
                case UP_TO_DATE:
                    // this index is good - skipping all other checks
                    return null;
                case NOT_APPLICABLE:
                    // this action is not applicable to this index - skipping to the next one
                    break;
                default:
                    throw new IllegalStateException("unknown upgrade action " + upgradeActionRequired + " for the index "
                            + index);

            }
        }
        // Catch all check for all indices that didn't match the specific checks
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0)) {
            return UpgradeActionRequired.REINDEX;
        } else {
            return null;
        }
    }

    public void upgrade(TaskId task, String index, ClusterState state, ActionListener<BulkByScrollResponse> listener) {
        IndexMetaData indexMetaData = state.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        for (IndexUpgradeCheck check : upgradeChecks) {
            UpgradeActionRequired upgradeActionRequired = check.actionRequired(indexMetaData);
            switch (upgradeActionRequired) {
                case UPGRADE:
                    // this index needs to be upgraded - start the upgrade procedure
                    check.upgrade(task, indexMetaData, state, listener);
                    return;
                case REINDEX:
                    // this index needs to be re-indexed
                    throw new IllegalStateException("Index [" + index + "] cannot be upgraded, it should be reindex instead");
                case UP_TO_DATE:
                    throw new IllegalStateException("Index [" + index + "] cannot be upgraded, it is up to date");
                case NOT_APPLICABLE:
                    // this action is not applicable to this index - skipping to the next one
                    break;
                default:
                    throw new IllegalStateException("unknown upgrade action [" + upgradeActionRequired + "] for the index [" + index + "]");

            }
        }
        throw new IllegalStateException("Index [" + index + "] cannot be upgraded");
    }

}
