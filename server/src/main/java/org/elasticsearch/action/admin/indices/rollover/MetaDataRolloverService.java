/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.findTemplates;

public class MetaDataRolloverService {
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-\\d+$");

    private final ThreadPool threadPool;
    private final MetaDataCreateIndexService createIndexService;
    private final MetaDataIndexAliasesService indexAliasesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public MetaDataRolloverService(ThreadPool threadPool,
                                   MetaDataCreateIndexService createIndexService, MetaDataIndexAliasesService indexAliasesService,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        this.threadPool = threadPool;
        this.createIndexService = createIndexService;
        this.indexAliasesService = indexAliasesService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public static class RolloverResult {
        public final String rolloverIndexName;
        public final String sourceIndexName;
        public final ClusterState clusterState;

        private RolloverResult(String rolloverIndexName, String sourceIndexName, ClusterState clusterState) {
            this.rolloverIndexName = rolloverIndexName;
            this.sourceIndexName = sourceIndexName;
            this.clusterState = clusterState;
        }
    }

    public RolloverResult rolloverClusterState(ClusterState currentState, String aliasName, String newIndexName,
                                               CreateIndexRequest createIndexRequest, List<Condition<?>> metConditions,
                                               boolean silent) throws Exception {
        final MetaData metaData = currentState.metaData();
        validate(metaData, aliasName);
        final AliasOrIndex.Alias alias = (AliasOrIndex.Alias) metaData.getAliasAndIndexLookup().get(aliasName);
        final IndexMetaData indexMetaData = alias.getWriteIndex();
        final AliasMetaData aliasMetaData = indexMetaData.getAliases().get(alias.getAliasName());
        final String sourceProvidedName = indexMetaData.getSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME,
            indexMetaData.getIndex().getName());
        final String sourceIndexName = indexMetaData.getIndex().getName();
        final String unresolvedName = (newIndexName != null)
            ? newIndexName
            : generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
        final String rolloverIndexName = indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        final boolean explicitWriteIndex = Boolean.TRUE.equals(aliasMetaData.writeIndex());
        final Boolean isHidden = IndexMetaData.INDEX_HIDDEN_SETTING.exists(createIndexRequest.settings()) ?
            IndexMetaData.INDEX_HIDDEN_SETTING.get(createIndexRequest.settings()) : null;
        createIndexService.validateIndexName(rolloverIndexName, currentState); // fails if the index already exists
        checkNoDuplicatedAliasInIndexTemplate(metaData, rolloverIndexName, aliasName, isHidden);

        CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest =
            prepareCreateIndexRequest(unresolvedName, rolloverIndexName, createIndexRequest);
        ClusterState newState = createIndexService.applyCreateIndexRequest(currentState, createIndexClusterStateRequest, silent);
        newState = indexAliasesService.applyAliasActions(newState,
            rolloverAliasToNewIndex(sourceIndexName, rolloverIndexName, explicitWriteIndex, aliasMetaData.isHidden(), aliasName));

        RolloverInfo rolloverInfo = new RolloverInfo(aliasName, metConditions, threadPool.absoluteTimeInMillis());
        newState = ClusterState.builder(newState)
            .metaData(MetaData.builder(newState.metaData())
                .put(IndexMetaData.builder(newState.metaData().index(sourceIndexName))
                    .putRolloverInfo(rolloverInfo))).build();

        return new RolloverResult(rolloverIndexName, sourceIndexName, newState);
    }

    static String generateRolloverIndexName(String sourceIndexName, IndexNameExpressionResolver indexNameExpressionResolver) {
        String resolvedName = indexNameExpressionResolver.resolveDateMathExpression(sourceIndexName);
        final boolean isDateMath = sourceIndexName.equals(resolvedName) == false;
        if (INDEX_NAME_PATTERN.matcher(resolvedName).matches()) {
            int numberIndex = sourceIndexName.lastIndexOf("-");
            assert numberIndex != -1 : "no separator '-' found";
            int counter = Integer.parseInt(sourceIndexName.substring(numberIndex + 1,
                isDateMath ? sourceIndexName.length()-1 : sourceIndexName.length()));
            String newName = sourceIndexName.substring(0, numberIndex) + "-" + String.format(Locale.ROOT, "%06d", ++counter)
                + (isDateMath ? ">" : "");
            return newName;
        } else {
            throw new IllegalArgumentException("index name [" + sourceIndexName + "] does not match pattern '^.*-\\d+$'");
        }
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final String providedIndexName, final String targetIndexName,
                                                                          CreateIndexRequest createIndexRequest) {
        createIndexRequest.cause("rollover_index");
        createIndexRequest.index(targetIndexName);
        return new CreateIndexClusterStateUpdateRequest(
            "rollover_index", targetIndexName, providedIndexName)
            .ackTimeout(createIndexRequest.timeout())
            .masterNodeTimeout(createIndexRequest.masterNodeTimeout())
            .settings(createIndexRequest.settings())
            .aliases(createIndexRequest.aliases())
            .waitForActiveShards(ActiveShardCount.NONE) // not waiting for shards here, will wait on the alias switch operation
            .mappings(createIndexRequest.mappings());
    }

    /**
     * Creates the alias actions to reflect the alias rollover from the old (source) index to the new (target/rolled over) index. An
     * alias pointing to multiple indices will have to be an explicit write index (ie. the old index alias has is_write_index set to true)
     * in which case, after the rollover, the new index will need to be the explicit write index.
     */
    static List<AliasAction> rolloverAliasToNewIndex(String oldIndex, String newIndex, boolean explicitWriteIndex,
                                                     @Nullable Boolean isHidden, String alias) {
        if (explicitWriteIndex) {
            return List.of(
                new AliasAction.Add(newIndex, alias, null, null, null, true, isHidden),
                new AliasAction.Add(oldIndex, alias, null, null, null, false, isHidden));
        } else {
            return List.of(
                new AliasAction.Add(newIndex, alias, null, null, null, null, isHidden),
                new AliasAction.Remove(oldIndex, alias));
        }
    }

    /**
     * If the newly created index matches with an index template whose aliases contains the rollover alias,
     * the rollover alias will point to multiple indices. This causes indexing requests to be rejected.
     * To avoid this, we make sure that there is no duplicated alias in index templates before creating a new index.
     */
    static void checkNoDuplicatedAliasInIndexTemplate(MetaData metaData, String rolloverIndexName, String rolloverRequestAlias,
                                                      @Nullable Boolean isHidden) {
        final List<IndexTemplateMetaData> matchedTemplates = findTemplates(metaData, rolloverIndexName, isHidden);
        for (IndexTemplateMetaData template : matchedTemplates) {
            if (template.aliases().containsKey(rolloverRequestAlias)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                    rolloverRequestAlias, template.aliases().keys(), template.name()));
            }
        }
    }

    static void validate(MetaData metaData, String aliasName) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(aliasName);
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("source alias does not exist");
        }
        if (aliasOrIndex.isAlias() == false) {
            throw new IllegalArgumentException("source alias is a concrete index");
        }
        final AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
        if (alias.getWriteIndex() == null) {
            throw new IllegalArgumentException("source alias [" + alias.getAliasName() + "] does not point to a write index");
        }
    }
}
