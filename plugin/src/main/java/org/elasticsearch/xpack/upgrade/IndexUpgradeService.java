/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexUpgradeService extends AbstractComponent {

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

}
