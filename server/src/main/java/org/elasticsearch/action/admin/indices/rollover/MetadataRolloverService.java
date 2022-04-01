/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.cluster.metadata.IndexAbstraction.Type.ALIAS;
import static org.elasticsearch.cluster.metadata.IndexAbstraction.Type.DATA_STREAM;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.lookupTemplateForDataStream;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV1Templates;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV2Template;

/**
 * Service responsible for handling rollover requests for write aliases and data streams
 */
public class MetadataRolloverService {
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-\\d+$");
    private static final List<IndexAbstraction.Type> VALID_ROLLOVER_TARGETS = List.of(ALIAS, DATA_STREAM);

    public static final Settings HIDDEN_INDEX_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build();

    private final ThreadPool threadPool;
    private final MetadataCreateIndexService createIndexService;
    private final MetadataIndexAliasesService indexAliasesService;
    private final SystemIndices systemIndices;

    @Inject
    public MetadataRolloverService(
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        MetadataIndexAliasesService indexAliasesService,
        SystemIndices systemIndices
    ) {
        this.threadPool = threadPool;
        this.createIndexService = createIndexService;
        this.indexAliasesService = indexAliasesService;
        this.systemIndices = systemIndices;
    }

    public record RolloverResult(String rolloverIndexName, String sourceIndexName, ClusterState clusterState) {

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "cluster state version [%d], rollover index name [%s], source index name [%s]",
                clusterState.version(),
                rolloverIndexName,
                sourceIndexName
            );
        }
    }

    public RolloverResult rolloverClusterState(
        ClusterState currentState,
        String rolloverTarget,
        String newIndexName,
        CreateIndexRequest createIndexRequest,
        List<Condition<?>> metConditions,
        Instant now,
        boolean silent,
        boolean onlyValidate
    ) throws Exception {
        validate(currentState.metadata(), rolloverTarget, newIndexName, createIndexRequest);
        final IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(rolloverTarget);
        return switch (indexAbstraction.getType()) {
            case ALIAS -> rolloverAlias(
                currentState,
                (IndexAbstraction.Alias) indexAbstraction,
                rolloverTarget,
                newIndexName,
                createIndexRequest,
                metConditions,
                silent,
                onlyValidate
            );
            case DATA_STREAM -> rolloverDataStream(
                currentState,
                (IndexAbstraction.DataStream) indexAbstraction,
                rolloverTarget,
                createIndexRequest,
                metConditions,
                now,
                silent,
                onlyValidate
            );
            default ->
                // the validate method above prevents this case
                throw new IllegalStateException("unable to roll over type [" + indexAbstraction.getType().getDisplayName() + "]");
        };
    }

    public static void validateIndexName(ClusterState state, String index) {
        MetadataCreateIndexService.validateIndexName(index, state);
    }

    /**
     * Returns the names that rollover would use, but does not perform the actual rollover
     */
    public static NameResolution resolveRolloverNames(
        ClusterState currentState,
        String rolloverTarget,
        String newIndexName,
        CreateIndexRequest createIndexRequest
    ) {
        validate(currentState.metadata(), rolloverTarget, newIndexName, createIndexRequest);
        final IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(rolloverTarget);
        return switch (indexAbstraction.getType()) {
            case ALIAS -> resolveAliasRolloverNames(currentState.metadata(), indexAbstraction, newIndexName);
            case DATA_STREAM -> resolveDataStreamRolloverNames(currentState.getMetadata(), (IndexAbstraction.DataStream) indexAbstraction);
            default ->
                // the validate method above prevents this case
                throw new IllegalStateException("unable to roll over type [" + indexAbstraction.getType().getDisplayName() + "]");
        };
    }

    public record NameResolution(String sourceName, @Nullable String unresolvedName, String rolloverName) {}

    private static NameResolution resolveAliasRolloverNames(Metadata metadata, IndexAbstraction alias, String newIndexName) {
        final IndexMetadata writeIndex = metadata.index(alias.getWriteIndex());
        final String sourceProvidedName = writeIndex.getSettings()
            .get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, writeIndex.getIndex().getName());
        final String sourceIndexName = writeIndex.getIndex().getName();
        final String unresolvedName = (newIndexName != null) ? newIndexName : generateRolloverIndexName(sourceProvidedName);
        final String rolloverIndexName = IndexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        return new NameResolution(sourceIndexName, unresolvedName, rolloverIndexName);
    }

    private static NameResolution resolveDataStreamRolloverNames(Metadata metadata, IndexAbstraction.DataStream dataStream) {
        final DataStream ds = dataStream.getDataStream();
        final IndexMetadata originalWriteIndex = metadata.index(dataStream.getWriteIndex());
        return new NameResolution(originalWriteIndex.getIndex().getName(), null, ds.nextWriteIndexAndGeneration(metadata).v1());
    }

    private RolloverResult rolloverAlias(
        ClusterState currentState,
        IndexAbstraction.Alias alias,
        String aliasName,
        String newIndexName,
        CreateIndexRequest createIndexRequest,
        List<Condition<?>> metConditions,
        boolean silent,
        boolean onlyValidate
    ) throws Exception {
        final NameResolution names = resolveAliasRolloverNames(currentState.metadata(), alias, newIndexName);
        final String sourceIndexName = names.sourceName;
        final String rolloverIndexName = names.rolloverName;
        final String unresolvedName = names.unresolvedName;
        final Metadata metadata = currentState.metadata();
        final IndexMetadata writeIndex = currentState.metadata().index(alias.getWriteIndex());
        final AliasMetadata aliasMetadata = writeIndex.getAliases().get(alias.getName());
        final boolean explicitWriteIndex = Boolean.TRUE.equals(aliasMetadata.writeIndex());
        final Boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.exists(createIndexRequest.settings())
            ? IndexMetadata.INDEX_HIDDEN_SETTING.get(createIndexRequest.settings())
            : null;
        MetadataCreateIndexService.validateIndexName(rolloverIndexName, currentState); // fails if the index already exists
        checkNoDuplicatedAliasInIndexTemplate(metadata, rolloverIndexName, aliasName, isHidden);
        if (onlyValidate) {
            return new RolloverResult(rolloverIndexName, sourceIndexName, currentState);
        }

        CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest = prepareCreateIndexRequest(
            unresolvedName,
            rolloverIndexName,
            createIndexRequest
        );
        ClusterState newState = createIndexService.applyCreateIndexRequest(currentState, createIndexClusterStateRequest, silent);
        newState = indexAliasesService.applyAliasActions(
            newState,
            rolloverAliasToNewIndex(sourceIndexName, rolloverIndexName, explicitWriteIndex, aliasMetadata.isHidden(), aliasName)
        );

        RolloverInfo rolloverInfo = new RolloverInfo(aliasName, metConditions, threadPool.absoluteTimeInMillis());
        newState = ClusterState.builder(newState)
            .metadata(
                Metadata.builder(newState.metadata())
                    .put(IndexMetadata.builder(newState.metadata().index(sourceIndexName)).putRolloverInfo(rolloverInfo))
            )
            .build();

        return new RolloverResult(rolloverIndexName, sourceIndexName, newState);
    }

    private RolloverResult rolloverDataStream(
        ClusterState currentState,
        IndexAbstraction.DataStream dataStream,
        String dataStreamName,
        CreateIndexRequest createIndexRequest,
        List<Condition<?>> metConditions,
        Instant now,
        boolean silent,
        boolean onlyValidate
    ) throws Exception {

        if (SnapshotsService.snapshottingDataStreams(currentState, Collections.singleton(dataStream.getName())).isEmpty() == false) {
            // we can't roll over the snapshot concurrently because the snapshot contains the indices that existed when it was started but
            // the cluster metadata of when it completes so the new write index would not exist in the snapshot if there was a concurrent
            // rollover
            throw new SnapshotInProgressException(
                "Cannot roll over data stream that is being snapshotted: "
                    + dataStream.getName()
                    + ". Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        final ComposableIndexTemplate templateV2;
        final SystemDataStreamDescriptor systemDataStreamDescriptor;
        if (dataStream.isSystem() == false) {
            systemDataStreamDescriptor = null;
            templateV2 = lookupTemplateForDataStream(dataStreamName, currentState.metadata());
        } else {
            systemDataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(dataStreamName);
            if (systemDataStreamDescriptor == null) {
                throw new IllegalArgumentException("no system data stream descriptor found for data stream [" + dataStreamName + "]");
            }
            templateV2 = systemDataStreamDescriptor.getComposableIndexTemplate();
        }

        final DataStream ds = dataStream.getDataStream();
        final Index originalWriteIndex = dataStream.getWriteIndex();
        final Tuple<String, Long> nextIndexAndGeneration = ds.nextWriteIndexAndGeneration(currentState.metadata());
        final String newWriteIndexName = nextIndexAndGeneration.v1();
        final long newGeneration = nextIndexAndGeneration.v2();
        MetadataCreateIndexService.validateIndexName(newWriteIndexName, currentState); // fails if the index already exists
        if (onlyValidate) {
            return new RolloverResult(newWriteIndexName, originalWriteIndex.getName(), currentState);
        }

        CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest = prepareDataStreamCreateIndexRequest(
            dataStreamName,
            newWriteIndexName,
            createIndexRequest,
            systemDataStreamDescriptor,
            now
        );
        createIndexClusterStateRequest.setMatchingTemplate(templateV2);
        ClusterState newState = createIndexService.applyCreateIndexRequest(
            currentState,
            createIndexClusterStateRequest,
            silent,
            (builder, indexMetadata) -> builder.put(
                ds.rollover(indexMetadata.getIndex(), newGeneration, templateV2.getDataStreamTemplate().getIndexMode())
            )
        );

        RolloverInfo rolloverInfo = new RolloverInfo(dataStreamName, metConditions, threadPool.absoluteTimeInMillis());
        newState = ClusterState.builder(newState)
            .metadata(
                Metadata.builder(newState.metadata())
                    .put(IndexMetadata.builder(newState.metadata().index(originalWriteIndex)).putRolloverInfo(rolloverInfo))
            )
            .build();

        return new RolloverResult(newWriteIndexName, originalWriteIndex.getName(), newState);
    }

    static String generateRolloverIndexName(String sourceIndexName) {
        String resolvedName = IndexNameExpressionResolver.resolveDateMathExpression(sourceIndexName);
        final boolean isDateMath = sourceIndexName.equals(resolvedName) == false;
        if (INDEX_NAME_PATTERN.matcher(resolvedName).matches()) {
            int numberIndex = sourceIndexName.lastIndexOf("-");
            assert numberIndex != -1 : "no separator '-' found";
            int counter = Integer.parseInt(
                sourceIndexName.substring(numberIndex + 1, isDateMath ? sourceIndexName.length() - 1 : sourceIndexName.length())
            );
            String newName = sourceIndexName.substring(0, numberIndex)
                + "-"
                + String.format(Locale.ROOT, "%06d", ++counter)
                + (isDateMath ? ">" : "");
            return newName;
        } else {
            throw new IllegalArgumentException("index name [" + sourceIndexName + "] does not match pattern '^.*-\\d+$'");
        }
    }

    static CreateIndexClusterStateUpdateRequest prepareDataStreamCreateIndexRequest(
        final String dataStreamName,
        final String targetIndexName,
        CreateIndexRequest createIndexRequest,
        final SystemDataStreamDescriptor descriptor,
        Instant now
    ) {
        Settings settings = descriptor != null ? Settings.EMPTY : HIDDEN_INDEX_SETTINGS;
        return prepareCreateIndexRequest(targetIndexName, targetIndexName, "rollover_data_stream", createIndexRequest, settings)
            .dataStreamName(dataStreamName)
            .nameResolvedInstant(now.toEpochMilli())
            .systemDataStreamDescriptor(descriptor);
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(
        final String providedIndexName,
        final String targetIndexName,
        CreateIndexRequest createIndexRequest
    ) {
        return prepareCreateIndexRequest(providedIndexName, targetIndexName, "rollover_index", createIndexRequest, null);
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(
        final String providedIndexName,
        final String targetIndexName,
        final String cause,
        CreateIndexRequest createIndexRequest,
        Settings settings
    ) {
        Settings.Builder b = Settings.builder().put(createIndexRequest.settings());
        if (settings != null) {
            b.put(settings);
        }
        return new CreateIndexClusterStateUpdateRequest(cause, targetIndexName, providedIndexName).ackTimeout(createIndexRequest.timeout())
            .masterNodeTimeout(createIndexRequest.masterNodeTimeout())
            .settings(b.build())
            .aliases(createIndexRequest.aliases())
            .waitForActiveShards(ActiveShardCount.NONE) // not waiting for shards here, will wait on the alias switch operation
            .mappings(createIndexRequest.mappings())
            .performReroute(false);
    }

    /**
     * Creates the alias actions to reflect the alias rollover from the old (source) index to the new (target/rolled over) index. An
     * alias pointing to multiple indices will have to be an explicit write index (ie. the old index alias has is_write_index set to true)
     * in which case, after the rollover, the new index will need to be the explicit write index.
     */
    static List<AliasAction> rolloverAliasToNewIndex(
        String oldIndex,
        String newIndex,
        boolean explicitWriteIndex,
        @Nullable Boolean isHidden,
        String alias
    ) {
        if (explicitWriteIndex) {
            return List.of(
                new AliasAction.Add(newIndex, alias, null, null, null, true, isHidden),
                new AliasAction.Add(oldIndex, alias, null, null, null, false, isHidden)
            );
        } else {
            return List.of(
                new AliasAction.Add(newIndex, alias, null, null, null, null, isHidden),
                new AliasAction.Remove(oldIndex, alias, null)
            );
        }
    }

    /**
     * If the newly created index matches with an index template whose aliases contains the rollover alias,
     * the rollover alias will point to multiple indices. This causes indexing requests to be rejected.
     * To avoid this, we make sure that there is no duplicated alias in index templates before creating a new index.
     */
    static void checkNoDuplicatedAliasInIndexTemplate(
        Metadata metadata,
        String rolloverIndexName,
        String rolloverRequestAlias,
        @Nullable Boolean isHidden
    ) {
        final String matchedV2Template = findV2Template(metadata, rolloverIndexName, isHidden == null ? false : isHidden);
        if (matchedV2Template != null) {
            List<Map<String, AliasMetadata>> aliases = MetadataIndexTemplateService.resolveAliases(metadata, matchedV2Template);
            for (Map<String, AliasMetadata> aliasConfig : aliases) {
                if (aliasConfig.containsKey(rolloverRequestAlias)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                            rolloverRequestAlias,
                            aliasConfig.keySet(),
                            matchedV2Template
                        )
                    );
                }
            }
            return;
        }

        final List<IndexTemplateMetadata> matchedTemplates = findV1Templates(metadata, rolloverIndexName, isHidden);
        for (IndexTemplateMetadata template : matchedTemplates) {
            if (template.aliases().containsKey(rolloverRequestAlias)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                        rolloverRequestAlias,
                        template.aliases().keySet(),
                        template.name()
                    )
                );
            }
        }
    }

    static void validate(Metadata metadata, String rolloverTarget, String newIndexName, CreateIndexRequest request) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(rolloverTarget);
        if (indexAbstraction == null) {
            throw new IllegalArgumentException("rollover target [" + rolloverTarget + "] does not exist");
        }
        if (VALID_ROLLOVER_TARGETS.contains(indexAbstraction.getType()) == false) {
            throw new IllegalArgumentException(
                "rollover target is a ["
                    + indexAbstraction.getType().getDisplayName()
                    + "] but one of ["
                    + Strings.collectionToCommaDelimitedString(
                        VALID_ROLLOVER_TARGETS.stream().map(IndexAbstraction.Type::getDisplayName).toList()
                    )
                    + "] was expected"
            );
        }
        if (indexAbstraction.getWriteIndex() == null) {
            throw new IllegalArgumentException("rollover target [" + indexAbstraction.getName() + "] does not point to a write index");
        }
        if (indexAbstraction.getType() == DATA_STREAM) {
            if (Strings.isNullOrEmpty(newIndexName) == false) {
                throw new IllegalArgumentException("new index name may not be specified when rolling over a data stream");
            }
            if ((request.settings().equals(Settings.EMPTY) == false)
                || (request.aliases().size() > 0)
                || (request.mappings().equals("{}") == false)) {
                throw new IllegalArgumentException(
                    "aliases, mappings, and index settings may not be specified when rolling over a data stream"
                );
            }
        }
    }
}
