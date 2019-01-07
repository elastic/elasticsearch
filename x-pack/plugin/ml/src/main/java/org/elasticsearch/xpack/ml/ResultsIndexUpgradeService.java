/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.action.MlUpgradeAction;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * ML Job results index upgrade service
 */
public class ResultsIndexUpgradeService {

    private static final Logger logger = LogManager.getLogger(ResultsIndexUpgradeService.class);

    // Adjust the following constants as necessary for various versions and backports.
    private static final int INDEX_VERSION = Version.CURRENT.major;
    private static final Version MIN_REQUIRED_VERSION = Version.CURRENT.minimumCompatibilityVersion();

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Predicate<IndexMetaData> shouldUpgrade;
    private final String executor;

    /**
     * Construct a new upgrade service
     *
     * @param indexNameExpressionResolver Index expression resolver for the request
     * @param executor                    Where to execute client calls
     * @param shouldUpgrade               Given IndexMetadata indicate if it should be upgraded or not
     *                                    {@code true} indicates that it SHOULD upgrade
     */
    public ResultsIndexUpgradeService(IndexNameExpressionResolver indexNameExpressionResolver,
                                      String executor,
                                      Predicate<IndexMetaData> shouldUpgrade) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.shouldUpgrade = shouldUpgrade;
        this.executor = executor;
    }

    public static boolean wasIndexCreatedInCurrentMajorVersion(IndexMetaData indexMetaData) {
        return indexMetaData.getCreationVersion().major == INDEX_VERSION;
    }

    /**
     * There are two reasons for these indices to exist:
     * 1. The upgrade process has ran before and either failed for some reason, or the end user is simply running it again.
     * Either way, it should be ok to proceed as this action SHOULD be idempotent,
     * unless the shouldUpgrade predicate is poorly formed
     * 2. This index was created manually by the user. If the index was created manually and actually needs upgrading, then
     * we consider the "new index" to be invalid as the passed predicate indicates that it still needs upgrading.
     *
     * @param metaData      Cluster metadata
     * @param newIndexName  The index to check
     * @param shouldUpgrade Should be index be upgraded
     * @return {@code true} if the "new index" is valid
     */
    private static boolean validNewIndex(MetaData metaData, String newIndexName, Predicate<IndexMetaData> shouldUpgrade) {
        return (metaData.hasIndex(newIndexName) && shouldUpgrade.test(metaData.index(newIndexName))) == false;
    }

    private static void validateMinNodeVersion(ClusterState clusterState) {
        if (clusterState.nodes().getMinNodeVersion().before(MIN_REQUIRED_VERSION)) {
            throw new IllegalStateException("All nodes should have at least version [" + MIN_REQUIRED_VERSION + "] to upgrade");
        }
    }

    // This method copies the behavior of the normal {index}/_upgrade rest response handler
    private static Tuple<RestStatus, Throwable> getStatusAndCause(BulkByScrollResponse response) {
        /*
         * Return the highest numbered rest status under the assumption that higher numbered statuses are "more error"
         * and thus more interesting to the user.
         */
        RestStatus status = RestStatus.OK;
        Throwable cause = null;
        if (response.isTimedOut()) {
            status = RestStatus.REQUEST_TIMEOUT;
            cause = new ElasticsearchTimeoutException("Reindex request timed out");
        }
        for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
                cause = failure.getCause();
            }
        }
        for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
            RestStatus failureStatus = ExceptionsHelper.status(failure.getReason());
            if (failureStatus.getStatus() > status.getStatus()) {
                status = failureStatus;
                cause = failure.getReason();
            }
        }
        return new Tuple<>(status, cause);
    }

    /**
     * Upgrade the indices given in the request.
     *
     * @param client   The client to use when making calls
     * @param request  The upgrade request
     * @param state    The current cluster state
     * @param listener The listener to alert when actions have completed
     */
    public void upgrade(Client client, MlUpgradeAction.Request request, ClusterState state,
                        ActionListener<AcknowledgedResponse> listener) {
        try {
            validateMinNodeVersion(state);
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.indices());
            MetaData metaData = state.getMetaData();

            List<String> indicesToUpgrade = Arrays.stream(concreteIndices)
                .filter(indexName -> shouldUpgrade.test(metaData.index(indexName)))
                .collect(Collectors.toList());

            // All the internal indices are up to date
            if (indicesToUpgrade.isEmpty()) {
                listener.onResponse(new AcknowledgedResponse(true));
                return;
            }

            IndexNameAndAliasProvider indexNameAndAliasProvider = new IndexNameAndAliasProvider(indicesToUpgrade, metaData);
            Exception validationException = indexNameAndAliasProvider.validate(metaData, shouldUpgrade);
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }

            // <7> Now that we have deleted the old indices, we are complete, alert the user
            ActionListener<AcknowledgedResponse> deleteIndicesListener = ActionListener.wrap(
                listener::onResponse,
                error -> {
                    String msg = "Failed to delete old indices: " + Strings.collectionToCommaDelimitedString(indicesToUpgrade);
                    logger.error(msg, error);
                    listener.onFailure(new ElasticsearchException(msg, error));
                }
            );

            // <6> Now that aliases are moved, need to delete the old indices
            ActionListener<AcknowledgedResponse> readAliasListener = ActionListener.wrap(
                resp -> deleteOldIndices(client, indicesToUpgrade, deleteIndicesListener),
                error -> {
                    String msg = "Failed adjusting aliases from old indices to new.";
                    logger.error(msg, error);
                    listener.onFailure(new ElasticsearchException(msg, error));
                }
            );

            // <5> Documents are now reindexed, time to move read aliases
            ActionListener<Boolean> reindexListener = ActionListener.wrap(
                resp ->
                    // Need to make indices writable again so that the aliases can be removed from them
                    removeReadOnlyBlock(client, indicesToUpgrade,
                        ActionListener.wrap(
                            rrob -> adjustAliases(client,
                                indexNameAndAliasProvider.oldIndicesWithReadAliases(),
                                indexNameAndAliasProvider.newReadIndicesWithReadAliases(),
                                readAliasListener),
                            rrobFailure -> {
                                String msg = "Failed making old indices writable again so that aliases can be moved.";
                                logger.error(msg, rrobFailure);
                                listener.onFailure(new ElasticsearchException(msg, rrobFailure));
                            })
                    ),
                error -> {
                    logger.error("Failed to reindex old read-only indices", error);
                    removeReadOnlyBlock(client, indicesToUpgrade, ActionListener.wrap(
                        empty -> listener.onFailure(error),
                        removeReadOnlyBlockError -> {
                            String msg = "Failed making old indices read/write again after failing to reindex: " + error.getMessage();
                            logger.error(msg, removeReadOnlyBlockError);
                            listener.onFailure(new ElasticsearchException(msg, removeReadOnlyBlockError));
                        }
                    ));
                }
            );

            // <4> Old indexes are now readOnly, Time to reindex
            ActionListener<AcknowledgedResponse> readOnlyListener = ActionListener.wrap(
                ack -> reindexOldReadIndicesToNewIndices(client, indexNameAndAliasProvider.needsReindex(), request, reindexListener),
                listener::onFailure
            );

            // <3> Set old indices to readOnly
            ActionListener<AcknowledgedResponse> writeAliasesMovedListener = ActionListener.wrap(
                resp -> setReadOnlyBlock(client, indicesToUpgrade, readOnlyListener),
                listener::onFailure
            );

            // <2> Move write index alias to new write indices
            ActionListener<AcknowledgedResponse> createWriteIndicesAndSetReadAliasListener = ActionListener.wrap(
                resp -> adjustAliases(client,
                    indexNameAndAliasProvider.oldIndicesWithWriteAliases(),
                    indexNameAndAliasProvider.newWriteIndicesWithWriteAliases(),
                    writeAliasesMovedListener),
                listener::onFailure
            );

            // <1> Create the new write indices and set the read aliases to include them
            createNewWriteIndicesIfNecessary(client, metaData, indexNameAndAliasProvider.newWriteIndices(),
                ActionListener.wrap(
                    indicesCreated -> adjustAliases(client,
                        Collections.emptyMap(),
                        indexNameAndAliasProvider.newWriteIndicesWithReadAliases(),
                        createWriteIndicesAndSetReadAliasListener),
                    listener::onFailure
                ));

        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    private void createNewWriteIndicesIfNecessary(Client client,
                                                  MetaData metaData,
                                                  Collection<String> newWriteIndices,
                                                  ActionListener<Boolean> createIndexListener) {
        TypedChainTaskExecutor<CreateIndexResponse> chainTaskExecutor =
            new TypedChainTaskExecutor<>(
                client.threadPool().executor(executor),
                (createIndexResponse -> true), //We always want to complete all our tasks
                (exception ->
                    // Short circuit execution IF the exception is NOT a ResourceAlreadyExistsException
                    // This should be rare, as it requires the index to be created between our previous check and this exception
                    exception instanceof ResourceAlreadyExistsException == false
                ));
        newWriteIndices.forEach((index) -> {
            // If the index already exists, don't try and created it
            // We have already verified that IF this index exists, that it does not require upgrading
            // So, if it was created between that check and this one, we can assume it is the correct version as it was JUST created
            if (metaData.hasIndex(index) == false) {
                CreateIndexRequest request = new CreateIndexRequest(index);
                chainTaskExecutor.add(listener ->
                    executeAsyncWithOrigin(client.threadPool().getThreadContext(),
                        ML_ORIGIN,
                        request,
                        listener,
                        client.admin().indices()::create));
            }
        });

        chainTaskExecutor.execute(ActionListener.wrap(
            createIndexResponses -> createIndexListener.onResponse(true),
            createIndexListener::onFailure
        ));
    }

    /**
     * Makes the indices readonly if it's not set as a readonly yet
     */
    private void setReadOnlyBlock(Client client, List<String> indices, ActionListener<AcknowledgedResponse> listener) {
        Settings settings = Settings.builder().put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), true).build();
        UpdateSettingsRequest request = new UpdateSettingsRequest(indices.toArray(new String[0]));
        request.settings(settings);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            request,
            listener,
            client.admin().indices()::updateSettings);
    }

    private void removeReadOnlyBlock(Client client, List<String> indices,
                                     ActionListener<AcknowledgedResponse> listener) {
        Settings settings = Settings.builder().put(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey(), false).build();
        UpdateSettingsRequest request = new UpdateSettingsRequest(indices.toArray(new String[0]));
        request.settings(settings);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            request,
            listener,
            client.admin().indices()::updateSettings);
    }

    private void reindexOldReadIndicesToNewIndices(Client client,
                                                   Map<String, String> reindexIndices,
                                                   MlUpgradeAction.Request request,
                                                   ActionListener<Boolean> listener) {
        TypedChainTaskExecutor<BulkByScrollResponse> chainTaskExecutor =
            new TypedChainTaskExecutor<>(
                client.threadPool().executor(executor),
                (createIndexResponse) -> { // If there are errors in the reindex, we should stop
                    Tuple<RestStatus, Throwable> status = getStatusAndCause(createIndexResponse);
                    return status.v1().equals(RestStatus.OK);
                },
                (exception -> true)); // Short circuit and call onFailure for any exception

        List<String> newIndices = new ArrayList<>(reindexIndices.size());
        reindexIndices.forEach((oldIndex, newIndex) -> {
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setSourceBatchSize(request.getReindexBatchSize());
            reindexRequest.setSourceIndices(oldIndex);
            reindexRequest.setDestIndex(newIndex);
            reindexRequest.setSourceDocTypes(ElasticsearchMappings.DOC_TYPE);
            reindexRequest.setDestDocType(ElasticsearchMappings.DOC_TYPE);
            // Don't worry if these indices already exist, we validated settings.index.created.version earlier
            reindexRequest.setAbortOnVersionConflict(false);
            // If the document exists already in the new index, don't want to update or overwrite as we are pulling from "old data"
            reindexRequest.setDestOpType(DocWriteRequest.OpType.CREATE.getLowercase());
            newIndices.add(newIndex);
            chainTaskExecutor.add(chainedListener ->
                executeAsyncWithOrigin(client,
                    ML_ORIGIN,
                    ReindexAction.INSTANCE,
                    reindexRequest,
                    chainedListener));
        });

        chainTaskExecutor.execute(ActionListener.wrap(
            bulkScrollingResponses -> {
                BulkByScrollResponse response = bulkScrollingResponses.get(bulkScrollingResponses.size() - 1);
                Tuple<RestStatus, Throwable> status = getStatusAndCause(response);
                if (status.v1().equals(RestStatus.OK)) {
                    listener.onResponse(true);
                } else {
                    logger.error("Failed to reindex old results indices.", status.v2());
                    listener.onFailure(new ElasticsearchException("Failed to reindex old results indices.",status.v2()));
                }
            },
            failure -> {
                List<String> createdIndices = newIndices.subList(0, chainTaskExecutor.getCollectedResponses().size());
                logger.error(
                    "Failed to reindex all old read indices. Successfully reindexed: [" +
                        Strings.collectionToCommaDelimitedString(createdIndices) + "]",
                    failure);
                listener.onFailure(failure);
            }
        ));

    }

    private void deleteOldIndices(Client client,
                                  List<String> oldIndices,
                                  ActionListener<AcknowledgedResponse> deleteIndicesListener) {
        DeleteIndexRequest request = new DeleteIndexRequest(oldIndices.toArray(new String[0]));
        request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            request,
            deleteIndicesListener,
            client.admin().indices()::delete);
    }

    private void adjustAliases(Client client,
                               Map<String, List<Alias>> oldAliases,
                               Map<String, List<Alias>> newAliases,
                               ActionListener<AcknowledgedResponse> indicesAliasListener) {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        oldAliases.forEach((oldIndex, aliases) ->
            {
                if (aliases.isEmpty() == false) { //if the aliases are empty, that means there are none to remove
                    indicesAliasesRequest.addAliasAction(IndicesAliasesRequest
                        .AliasActions
                        .remove()
                        .index(oldIndex)
                        .aliases(aliases.stream().map(Alias::name).toArray(String[]::new)));
                }
            }
        );
        newAliases.forEach((newIndex, aliases) ->
            aliases.forEach(alias -> {
                IndicesAliasesRequest.AliasActions action = IndicesAliasesRequest.AliasActions.add().index(newIndex);
                if (alias.filter() != null) {
                    action.filter(alias.filter());
                }
                action.alias(alias.name());
                indicesAliasesRequest.addAliasAction(action);
            })
        );
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            indicesAliasesRequest,
            indicesAliasListener,
            client.admin().indices()::aliases);
    }


    private static class IndexNameAndAliasProvider {

        private final List<String> oldIndices;
        private final Map<String, List<Alias>> writeAliases = new HashMap<>();
        private final Map<String, List<Alias>> readAliases = new HashMap<>();

        private IndexNameAndAliasProvider(List<String> oldIndices, MetaData metaData) {
            this.oldIndices = oldIndices;
            oldIndices.forEach(index -> {
                IndexMetaData indexMetaData = metaData.index(index);
                List<Alias> writes = new ArrayList<>();
                List<Alias> reads = new ArrayList<>();
                indexMetaData.getAliases().forEach(aliasCursor -> {
                    Alias alias = new Alias(aliasCursor.value.alias());
                    if (aliasCursor.value.filteringRequired()) {
                        alias.filter(aliasCursor.value.getFilter().string()); //Set the read alias jobId filter
                    }
                    if (alias.name().contains(".write-")) {
                        writes.add(alias);
                    } else {
                        reads.add(alias);
                    }
                });

                writeAliases.put(index, writes);
                readAliases.put(index, reads);
            });
        }

        private Exception validate(MetaData metaData, Predicate<IndexMetaData> shouldUpgrade) {
            for (String index : oldIndices) {
                String newWriteName = newWriteName(index);
                // If the "new" indices exist, either they were created from a previous run of the upgrade process or the end user
                if (validNewIndex(metaData, newWriteName, shouldUpgrade) == false) {
                    return new IllegalStateException("Index [" + newWriteName + "] already exists and is not the current version.");
                }

                String newReadName = newReadName(index);
                if (validNewIndex(metaData, newReadName, shouldUpgrade) == false) {
                    return new IllegalStateException("Index [" + newReadName + "] already exists and is not the current version.");
                }
            }
            return null;
        }

        private String newReadName(String oldIndexName) {
            return oldIndexName + "-" + INDEX_VERSION + "r";
        }

        private String newWriteName(String oldIndexName) {
            return oldIndexName + "-" + INDEX_VERSION;
        }

        private List<String> newWriteIndices() {
            return oldIndices.stream().map(this::newWriteName).collect(Collectors.toList());
        }

        private List<Alias> readAliases(String oldIndex) {
            return readAliases.get(oldIndex);
        }

        private List<Alias> writeAliases(String oldIndex) {
            return writeAliases.get(oldIndex);
        }

        private Map<String, List<Alias>> newWriteIndicesWithReadAliases() {
            return oldIndices.stream().collect(Collectors.toMap(this::newWriteName, this::readAliases));
        }

        private Map<String, List<Alias>> oldIndicesWithWriteAliases() {
            return writeAliases;
        }

        private Map<String, List<Alias>> newWriteIndicesWithWriteAliases() {
            return oldIndices.stream().collect(Collectors.toMap(this::newWriteName, this::writeAliases));
        }

        private Map<String, List<Alias>> oldIndicesWithReadAliases() {
            return readAliases;
        }

        private Map<String, List<Alias>> newReadIndicesWithReadAliases() {
            return oldIndices.stream().collect(Collectors.toMap(this::newReadName, this::readAliases));
        }

        private Map<String, String> needsReindex() {
            return oldIndices.stream().collect(Collectors.toMap(Function.identity(), this::newReadName));
        }
    }
}
