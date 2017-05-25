/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexUpgradeService extends AbstractComponent {
    public static final IndicesOptions UPGRADE_INDEX_OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private final List<IndexUpgradeCheck> upgradeChecks;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexUpgradeService(Settings settings, List<IndexUpgradeCheck> upgradeChecks) {
        super(settings);
        this.upgradeChecks = upgradeChecks;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(settings);
    }

    /**
     * Returns the information about required upgrade action for the given indices
     *
     * @param indices list of indices to check, specify _all for all indices
     * @param options wild card resolution option
     * @param params  list of additional parameters that will be passed to upgrade checks
     * @param state   the current cluster state
     * @return a list of indices that should be upgraded/reindexed
     */
    public Map<String, UpgradeActionRequired> upgradeInfo(String[] indices, IndicesOptions options, Map<String, String> params,
                                                          ClusterState state) {
        Map<String, UpgradeActionRequired> results = new HashMap<>();
        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, options, indices);
        MetaData metaData = state.getMetaData();
        for (String index : concreteIndexNames) {
            IndexMetaData indexMetaData = metaData.index(index);
            indexCheck:
            for (IndexUpgradeCheck check : upgradeChecks) {
                UpgradeActionRequired upgradeActionRequired = check.actionRequired(indexMetaData, params, state);
                logger.trace("[{}] check [{}] returned [{}]", index, check.getName(), upgradeActionRequired);
                switch (upgradeActionRequired) {
                    case UPGRADE:
                    case REINDEX:
                        // this index needs to be upgraded or reindexed - skipping all other checks
                        results.put(index, upgradeActionRequired);
                        break indexCheck;
                    case UP_TO_DATE:
                        // this index is good - skipping all other checks
                        break indexCheck;
                    case NOT_APPLICABLE:
                        // this action is not applicable to this index - skipping to the next one
                        break;
                    default:
                        throw new IllegalStateException("unknown upgrade action " + upgradeActionRequired + " for the index "
                                + index);

                }
            }
        }
        return results;
    }

    public void upgrade(String index, Map<String, String> params, ClusterState state,
                        ActionListener<BulkByScrollResponse> listener) {
        IndexMetaData indexMetaData = state.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        for (IndexUpgradeCheck check : upgradeChecks) {
            UpgradeActionRequired upgradeActionRequired = check.actionRequired(indexMetaData, params, state);
            switch (upgradeActionRequired) {
                case UPGRADE:
                    // this index needs to be upgraded - start the upgrade procedure
                    check.upgrade(indexMetaData, params, state, listener);
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
