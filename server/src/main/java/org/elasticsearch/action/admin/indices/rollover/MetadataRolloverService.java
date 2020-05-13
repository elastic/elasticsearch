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
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexAbstraction.Type.ALIAS;
import static org.elasticsearch.cluster.metadata.IndexAbstraction.Type.DATA_STREAM;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV1Templates;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV2Template;

/**
 * Service responsible for handling rollover requests for write aliases and data streams
 */
public class MetadataRolloverService {
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-\\d+$");
    private static final List<IndexAbstraction.Type> VALID_ROLLOVER_TARGETS = List.of(ALIAS, DATA_STREAM);

    private final ThreadPool threadPool;
    private final MetadataCreateIndexService createIndexService;
    private final MetadataIndexAliasesService indexAliasesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public MetadataRolloverService(ThreadPool threadPool,
                                   MetadataCreateIndexService createIndexService, MetadataIndexAliasesService indexAliasesService,
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

    public RolloverResult rolloverClusterState(ClusterState currentState, String rolloverTarget, String newIndexName,
                                               CreateIndexRequest createIndexRequest, List<Condition<?>> metConditions,
                                               boolean silent) throws Exception {
        validate(currentState.metadata(), rolloverTarget, newIndexName, createIndexRequest);
        final IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(rolloverTarget);
        switch (indexAbstraction.getType()) {
            case ALIAS:
                return rolloverAlias(currentState, (IndexAbstraction.Alias) indexAbstraction, rolloverTarget, newIndexName,
                    createIndexRequest, metConditions, silent);
            case DATA_STREAM:
                return  rolloverDataStream(currentState, (IndexAbstraction.DataStream) indexAbstraction, rolloverTarget,
                    createIndexRequest, metConditions, silent);
            default:
                // the validate method above prevents this case
                throw new IllegalStateException("unable to roll over type [" + indexAbstraction.getType().getDisplayName() + "]");
        }
    }

    private RolloverResult rolloverAlias(ClusterState currentState, IndexAbstraction.Alias alias, String aliasName,
                                         String newIndexName, CreateIndexRequest createIndexRequest, List<Condition<?>> metConditions,
                                         boolean silent) throws Exception {
        final Metadata metadata = currentState.metadata();
        final IndexMetadata writeIndex = alias.getWriteIndex();
        final AliasMetadata aliasMetadata = writeIndex.getAliases().get(alias.getName());
        final String sourceProvidedName = writeIndex.getSettings().get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME,
            writeIndex.getIndex().getName());
        final String sourceIndexName = writeIndex.getIndex().getName();
        final String unresolvedName = (newIndexName != null)
            ? newIndexName
            : generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
        final String rolloverIndexName = indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        final boolean explicitWriteIndex = Boolean.TRUE.equals(aliasMetadata.writeIndex());
        final Boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.exists(createIndexRequest.settings()) ?
            IndexMetadata.INDEX_HIDDEN_SETTING.get(createIndexRequest.settings()) : null;
        createIndexService.validateIndexName(rolloverIndexName, currentState); // fails if the index already exists
        checkNoDuplicatedAliasInIndexTemplate(metadata, rolloverIndexName, aliasName, isHidden);

        CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest =
            prepareCreateIndexRequest(unresolvedName, rolloverIndexName, createIndexRequest);
        ClusterState newState = createIndexService.applyCreateIndexRequest(currentState, createIndexClusterStateRequest, silent);
        newState = indexAliasesService.applyAliasActions(newState,
            rolloverAliasToNewIndex(sourceIndexName, rolloverIndexName, explicitWriteIndex, aliasMetadata.isHidden(), aliasName));

        RolloverInfo rolloverInfo = new RolloverInfo(aliasName, metConditions, threadPool.absoluteTimeInMillis());
        newState = ClusterState.builder(newState)
            .metadata(Metadata.builder(newState.metadata())
                .put(IndexMetadata.builder(newState.metadata().index(sourceIndexName))
                    .putRolloverInfo(rolloverInfo))).build();

        return new RolloverResult(rolloverIndexName, sourceIndexName, newState);
    }

    private RolloverResult rolloverDataStream(ClusterState currentState, IndexAbstraction.DataStream dataStream, String dataStreamName,
                                              CreateIndexRequest createIndexRequest, List<Condition<?>> metConditions,
                                              boolean silent) throws Exception {
        final DataStream ds = dataStream.getDataStream();
        final IndexMetadata originalWriteIndex = dataStream.getWriteIndex();
        final String newWriteIndexName = DataStream.getBackingIndexName(ds.getName(), ds.getGeneration() + 1);

        CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest =
            prepareDataStreamCreateIndexRequest(newWriteIndexName, createIndexRequest);
        ClusterState newState = createIndexService.applyCreateIndexRequest(currentState, createIndexClusterStateRequest, silent,
            (builder, indexMetadata) -> builder.put(ds.rollover(indexMetadata.getIndex())));

        RolloverInfo rolloverInfo = new RolloverInfo(dataStreamName, metConditions, threadPool.absoluteTimeInMillis());
        newState = ClusterState.builder(newState)
            .metadata(Metadata.builder(newState.metadata())
                .put(IndexMetadata.builder(newState.metadata().index(originalWriteIndex.getIndex()))
                    .putRolloverInfo(rolloverInfo)))
            .build();

        return new RolloverResult(newWriteIndexName, originalWriteIndex.getIndex().getName(), newState);
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

    static CreateIndexClusterStateUpdateRequest prepareDataStreamCreateIndexRequest(final String targetIndexName,
                                                                                    CreateIndexRequest createIndexRequest) {
        Settings settings = Settings.builder().put("index.hidden", true).build();
        return prepareCreateIndexRequest(targetIndexName, targetIndexName, "rollover_data_stream", createIndexRequest, settings);
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(
        final String providedIndexName, final String targetIndexName, CreateIndexRequest createIndexRequest) {
        return prepareCreateIndexRequest(providedIndexName, targetIndexName, "rollover_index", createIndexRequest, null);
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final String providedIndexName, final String targetIndexName,
                                                                          final String cause, CreateIndexRequest createIndexRequest,
                                                                          Settings settings) {
        Settings.Builder b = Settings.builder().put(createIndexRequest.settings());
        if (settings != null) {
            b.put(settings);
        }
        return new CreateIndexClusterStateUpdateRequest(cause, targetIndexName, providedIndexName)
            .ackTimeout(createIndexRequest.timeout())
            .masterNodeTimeout(createIndexRequest.masterNodeTimeout())
            .settings(b.build())
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
    static void checkNoDuplicatedAliasInIndexTemplate(Metadata metadata, String rolloverIndexName, String rolloverRequestAlias,
                                                      @Nullable Boolean isHidden) {
        final List<IndexTemplateMetadata> matchedTemplates = findV1Templates(metadata, rolloverIndexName, isHidden);
        for (IndexTemplateMetadata template : matchedTemplates) {
            if (template.aliases().containsKey(rolloverRequestAlias)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                    rolloverRequestAlias, template.aliases().keys(), template.name()));
            }
        }

        final String matchedV2Template = findV2Template(metadata, rolloverIndexName, isHidden == null ? false : isHidden);
        if (matchedV2Template != null) {
            List<Map<String, AliasMetadata>> aliases = MetadataIndexTemplateService.resolveAliases(metadata, matchedV2Template);
            for (Map<String, AliasMetadata> aliasConfig : aliases) {
                if (aliasConfig.containsKey(rolloverRequestAlias)) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                        rolloverRequestAlias, aliasConfig.keySet(), matchedV2Template));
                }
            }
        }
    }

    static void validate(Metadata metadata, String rolloverTarget, String newIndexName, CreateIndexRequest request) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(rolloverTarget);
        if (indexAbstraction == null) {
            throw new IllegalArgumentException("rollover target [" + rolloverTarget + "] does not exist");
        }
        if (VALID_ROLLOVER_TARGETS.contains(indexAbstraction.getType()) == false) {
            throw new IllegalArgumentException("rollover target is a [" + indexAbstraction.getType().getDisplayName() + "] but one of [" +
                Strings.collectionToCommaDelimitedString(VALID_ROLLOVER_TARGETS.stream().map(IndexAbstraction.Type::getDisplayName)
                    .collect(Collectors.toList())) + "] was expected");
        }
        if (indexAbstraction.getWriteIndex() == null) {
            throw new IllegalArgumentException(
                "rollover target [" + indexAbstraction.getName() + "] does not point to a write index");
        }
        if (indexAbstraction.getType() == DATA_STREAM) {
            if (Strings.isNullOrEmpty(newIndexName) == false) {
                throw new IllegalArgumentException("new index name may not be specified when rolling over a data stream");
            }
            if ((request.settings().equals(Settings.EMPTY) == false) ||
                (request.aliases().size() > 0) ||
                (request.mappings().equals("{}") == false)) {
                throw new IllegalArgumentException(
                    "aliases, mappings, and index settings may not be specified when rolling over a data stream");
            }
        }
    }
}
