/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Utils to create an ML index with alias ready for rollover with a 6-digit suffix
 */
public final class MlIndexAndAlias {

    /**
     * ML managed index mappings used to be updated based on the product version.
     * They are now updated based on per-index mappings versions. However, older
     * nodes will still look for a product version in the mappings metadata, so
     * we have to put <em>something</em> in that field that will allow the older
     * node to realise that the mappings are ahead of what it knows about. The
     * easiest solution is to hardcode 8.11.0 in this field, because any node
     * from 8.10.0 onwards should be using per-index mappings versions to determine
     * whether mappings are up-to-date.
     */
    public static final String BWC_MAPPINGS_VERSION = "8.11.0";

    public static final String FIRST_INDEX_SIX_DIGIT_SUFFIX = "-000001";

    private static final Logger logger = LogManager.getLogger(MlIndexAndAlias.class);
    private static final Predicate<String> HAS_SIX_DIGIT_SUFFIX = Pattern.compile("\\d{6}").asMatchPredicate();
    private static final Predicate<String> IS_ANOMALIES_SHARED_INDEX = Pattern.compile(
        AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT + "-\\d{6}"
    ).asMatchPredicate();
    private static final Predicate<String> IS_ANOMALIES_STATE_INDEX = Pattern.compile(
        AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-\\d{6}"
    ).asMatchPredicate();
    public static final String ROLLOVER_ALIAS_SUFFIX = ".rollover_alias";

    static final Comparator<String> INDEX_NAME_COMPARATOR = (index1, index2) -> {
        String[] index1Parts = index1.split("-");
        String index1Suffix = index1Parts[index1Parts.length - 1];
        boolean index1HasSixDigitsSuffix = HAS_SIX_DIGIT_SUFFIX.test(index1Suffix);
        String[] index2Parts = index2.split("-");
        String index2Suffix = index2Parts[index2Parts.length - 1];
        boolean index2HasSixDigitsSuffix = HAS_SIX_DIGIT_SUFFIX.test(index2Suffix);
        if (index1HasSixDigitsSuffix && index2HasSixDigitsSuffix) {
            return index1Suffix.compareTo(index2Suffix);
        } else if (index1HasSixDigitsSuffix != index2HasSixDigitsSuffix) {
            return Boolean.compare(index1HasSixDigitsSuffix, index2HasSixDigitsSuffix);
        } else {
            return index1.compareTo(index2);
        }
    };

    private MlIndexAndAlias() {}

    /**
     * Creates the first index with a name of the given {@code indexPatternPrefix} followed by "-000001", if the index is missing.
     * Adds an {@code alias} to that index if it was created,
     * or to the index with the highest suffix if the index did not have to be created.
     * The listener is notified with a {@code boolean} that informs whether the index or the alias were created.
     * If the index is created, the listener is not called until the index is ready to use via the supplied alias,
     * so that a method that receives a success response from this method can safely use the index immediately.
     */
    public static void createIndexAndAliasIfNecessary(
        Client client,
        ClusterState clusterState,
        IndexNameExpressionResolver resolver,
        String indexPatternPrefix,
        String alias,
        TimeValue masterNodeTimeout,
        ActiveShardCount waitForShardCount,
        ActionListener<Boolean> finalListener
    ) {
        createIndexAndAliasIfNecessary(
            client,
            clusterState,
            resolver,
            indexPatternPrefix,
            FIRST_INDEX_SIX_DIGIT_SUFFIX,
            alias,
            masterNodeTimeout,
            waitForShardCount,
            finalListener
        );
    }

    /**
     * Same as createIndexAndAliasIfNecessary but with the first concrete
     * index number specified.
     */
    public static void createIndexAndAliasIfNecessary(
        Client client,
        ClusterState clusterState,
        IndexNameExpressionResolver resolver,
        String indexPatternPrefix,
        String indexNumber,
        String alias,
        TimeValue masterNodeTimeout,
        ActiveShardCount waitForShardCount,
        ActionListener<Boolean> finalListener
    ) {

        final ActionListener<Boolean> loggingListener = ActionListener.wrap(finalListener::onResponse, e -> {
            logger.error(() -> format("Failed to create alias and index with pattern [%s] and alias [%s]", indexPatternPrefix, alias), e);
            finalListener.onFailure(e);
        });

        // If both the index and alias were successfully created then wait for the shards of the index that the alias points to be ready
        ActionListener<Boolean> indexCreatedListener = loggingListener.delegateFailureAndWrap((delegate, created) -> {
            if (created) {
                waitForShardsReady(client, alias, masterNodeTimeout, delegate);
            } else {
                delegate.onResponse(false);
            }
        });

        String legacyIndexWithoutSuffix = indexPatternPrefix;
        String indexPattern = indexPatternPrefix + "*";
        // The initial index name must be suitable for rollover functionality.
        String firstConcreteIndex = indexPatternPrefix + indexNumber;
        String[] concreteIndexNames = resolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandHidden(), indexPattern);
        Optional<String> indexPointedByCurrentWriteAlias = clusterState.getMetadata().getProject().hasAlias(alias)
            ? clusterState.getMetadata().getProject().getIndicesLookup().get(alias).getIndices().stream().map(Index::getName).findFirst()
            : Optional.empty();

        if (concreteIndexNames.length == 0) {
            if (indexPointedByCurrentWriteAlias.isEmpty()) {
                createFirstConcreteIndex(
                    client,
                    firstConcreteIndex,
                    alias,
                    true,
                    waitForShardCount,
                    masterNodeTimeout,
                    indexCreatedListener
                );
                return;
            }
            logger.error(
                "There are no indices matching '{}' pattern but '{}' alias points at [{}]. This should never happen.",
                indexPattern,
                alias,
                indexPointedByCurrentWriteAlias.get()
            );
        } else if (concreteIndexNames.length == 1 && concreteIndexNames[0].equals(legacyIndexWithoutSuffix)) {
            if (indexPointedByCurrentWriteAlias.isEmpty()) {
                createFirstConcreteIndex(
                    client,
                    firstConcreteIndex,
                    alias,
                    true,
                    waitForShardCount,
                    masterNodeTimeout,
                    indexCreatedListener
                );
                return;
            }
            if (indexPointedByCurrentWriteAlias.get().equals(legacyIndexWithoutSuffix)) {
                createFirstConcreteIndex(
                    client,
                    firstConcreteIndex,
                    alias,
                    false,
                    waitForShardCount,
                    masterNodeTimeout,
                    indexCreatedListener.delegateFailureAndWrap(
                        (l, unused) -> updateWriteAlias(client, alias, legacyIndexWithoutSuffix, firstConcreteIndex, masterNodeTimeout, l)
                    )
                );
                return;
            }
            logger.error(
                "There is exactly one index (i.e. '{}') matching '{}' pattern but '{}' alias points at [{}]. This should never happen.",
                legacyIndexWithoutSuffix,
                indexPattern,
                alias,
                indexPointedByCurrentWriteAlias.get()
            );
        } else {
            if (indexPointedByCurrentWriteAlias.isEmpty()) {
                assert concreteIndexNames.length > 0;
                String latestConcreteIndexName = latestIndex(concreteIndexNames);
                updateWriteAlias(client, alias, null, latestConcreteIndexName, masterNodeTimeout, loggingListener);
                return;
            }
        }
        // If the alias is set, there is nothing more to do.
        loggingListener.onResponse(false);
    }

    /**
     * Creates a system index based on the provided descriptor if it does not already exist.
     * <p>
     * The check for existence is simple and will return the listener on the calling thread if successful.
     * If the index needs to be created an async call will be made and this method will wait for the index to reach at least
     * a yellow health status before notifying the listener, ensuring it is ready for use
     * upon a successful response. A {@link ResourceAlreadyExistsException} during creation
     * is handled gracefully and treated as a success.
     *
     * @param client            The client to use for the create index request.
     * @param clusterState      The current cluster state, used for the initial existence check.
     * @param descriptor        The descriptor containing the index name, settings, and mappings.
     * @param masterNodeTimeout The timeout for waiting on the master node.
     * @param finalListener     Async listener
     */
    public static void createSystemIndexIfNecessary(
        Client client,
        ClusterState clusterState,
        SystemIndexDescriptor descriptor,
        TimeValue masterNodeTimeout,
        ActionListener<Boolean> finalListener
    ) {

        final String primaryIndex = descriptor.getPrimaryIndex();

        // The check for existence of the index is against the cluster state, so very cheap
        if (clusterState.getMetadata().getProject().hasIndexAbstraction(primaryIndex)) {
            finalListener.onResponse(true);
            return;
        }

        ActionListener<Boolean> indexCreatedListener = ActionListener.wrap(created -> {
            if (created) {
                waitForShardsReady(client, primaryIndex, masterNodeTimeout, finalListener);
            } else {
                finalListener.onResponse(false);
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                finalListener.onResponse(true);
            } else {
                finalListener.onFailure(e);
            }
        });

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(primaryIndex);
        createIndexRequest.settings(descriptor.getSettings());
        createIndexRequest.mapping(descriptor.getMappings());
        createIndexRequest.origin(ML_ORIGIN);
        createIndexRequest.masterNodeTimeout(masterNodeTimeout);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            createIndexRequest,
            indexCreatedListener.<CreateIndexResponse>delegateFailureAndWrap((l, r) -> l.onResponse(r.isAcknowledged())),
            client.admin().indices()::create
        );
    }

    private static void waitForShardsReady(Client client, String index, TimeValue masterNodeTimeout, ActionListener<Boolean> listener) {
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(masterNodeTimeout, index).waitForYellowStatus()
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            healthRequest,
            listener.<ClusterHealthResponse>delegateFailureAndWrap((l, response) -> l.onResponse(response.isTimedOut() == false)),
            client.admin().cluster()::health
        );
    }

    private static void createFirstConcreteIndex(
        Client client,
        String index,
        String alias,
        boolean addAlias,
        ActiveShardCount waitForShardCount,
        TimeValue masterNodeTimeout,
        ActionListener<Boolean> listener
    ) {
        logger.info("About to create first concrete index [{}] with alias [{}]", index, alias);
        CreateIndexRequestBuilder requestBuilder = client.admin().indices().prepareCreate(index);
        if (addAlias) {
            requestBuilder.addAlias(new Alias(alias).isHidden(true));
        }
        requestBuilder.setWaitForActiveShards(waitForShardCount);
        CreateIndexRequest request = requestBuilder.request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            request,
            ActionListener.<CreateIndexResponse>wrap(createIndexResponse -> listener.onResponse(true), createIndexFailure -> {
                if (ExceptionsHelper.unwrapCause(createIndexFailure) instanceof ResourceAlreadyExistsException) {
                    // If it was created between our last check and this request being handled, we should add the alias
                    // if we were asked to add it on creation. Adding an alias that already exists is idempotent. So
                    // no need to double check if the alias exists as well. But if we weren't asked to add the alias
                    // on creation then we should leave it up to the caller to decide what to do next (some call sites
                    // already have more advanced alias update logic in their success handlers).
                    if (addAlias) {
                        updateWriteAlias(client, alias, null, index, masterNodeTimeout, listener);
                    } else {
                        listener.onResponse(true);
                    }
                } else {
                    listener.onFailure(createIndexFailure);
                }
            }),
            client.admin().indices()::create
        );
    }

    /**
     * Creates or moves a write alias from one index to another.
     *
     * @param client            The client to use for the add alias request.
     * @param alias             The alias to update.
     * @param currentIndex      The index the alias is currently pointing to.
     * @param newIndex          The new index the alias should point to.
     * @param masterNodeTimeout The timeout for waiting on the master node.
     * @param listener          Async listener
     */
    public static void updateWriteAlias(
        Client client,
        String alias,
        @Nullable String currentIndex,
        String newIndex,
        TimeValue masterNodeTimeout,
        ActionListener<Boolean> listener
    ) {
        if (currentIndex != null) {
            logger.info("About to move write alias [{}] from index [{}] to index [{}]", alias, currentIndex, newIndex);
        } else {
            logger.info("About to create write alias [{}] for index [{}]", alias, newIndex);
        }
        IndicesAliasesRequestBuilder requestBuilder = client.admin()
            .indices()
            .prepareAliases(masterNodeTimeout, masterNodeTimeout)
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(newIndex).alias(alias).isHidden(true).writeIndex(true));
        if (currentIndex != null) {
            requestBuilder.removeAlias(currentIndex, alias);
        }
        IndicesAliasesRequest request = requestBuilder.request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            request,
            listener.<IndicesAliasesResponse>delegateFailureAndWrap((l, resp) -> l.onResponse(resp.isAcknowledged())),
            client.admin().indices()::aliases
        );
    }

    /**
     * Installs the index template specified by {@code templateConfig} if it is not in already
     * installed in {@code clusterState}.
     * <p>
     * The check for presence is simple and will return the listener on
     * the calling thread if successful. If the template has to be installed
     * an async call will be made.
     *
     * @param clusterState The cluster state
     * @param client For putting the template
     * @param templateConfig The config
     * @param listener Async listener
     */
    public static void installIndexTemplateIfRequired(
        ClusterState clusterState,
        Client client,
        IndexTemplateConfig templateConfig,
        TimeValue masterTimeout,
        ActionListener<Boolean> listener
    ) {
        String templateName = templateConfig.getTemplateName();

        // The check for existence of the template is against the cluster state, so very cheap
        if (hasIndexTemplate(clusterState, templateName, templateConfig.getVersion())) {
            listener.onResponse(true);
            return;
        }

        TransportPutComposableIndexTemplateAction.Request request;
        try {
            request = new TransportPutComposableIndexTemplateAction.Request(templateConfig.getTemplateName()).indexTemplate(
                templateConfig.load(ComposableIndexTemplate::parse)
            ).masterNodeTimeout(masterTimeout);
        } catch (IOException e) {
            throw new ElasticsearchParseException("unable to parse composable template " + templateConfig.getTemplateName(), e);
        }

        installIndexTemplateIfRequired(clusterState, client, templateConfig.getVersion(), request, listener);
    }

    /**
     * See {@link #installIndexTemplateIfRequired(ClusterState, Client, IndexTemplateConfig, TimeValue, ActionListener)}.
     *
     * Overload takes a {@code PutIndexTemplateRequest} instead of {@code IndexTemplateConfig}
     *
     * @param clusterState The cluster state
     * @param client For putting the template
     * @param templateRequest The Put template request
     * @param listener Async listener
     */
    public static void installIndexTemplateIfRequired(
        ClusterState clusterState,
        Client client,
        int templateVersion,
        TransportPutComposableIndexTemplateAction.Request templateRequest,
        ActionListener<Boolean> listener
    ) {
        // The check for existence of the template is against the cluster state, so very cheap
        if (hasIndexTemplate(clusterState, templateRequest.name(), templateVersion)) {
            listener.onResponse(true);
            return;
        }

        ActionListener<AcknowledgedResponse> innerListener = listener.delegateFailureAndWrap((l, response) -> {
            if (response.isAcknowledged() == false) {
                logger.warn("error adding template [{}], request was not acknowledged", templateRequest.name());
            }
            l.onResponse(response.isAcknowledged());
        });

        executeAsyncWithOrigin(client, ML_ORIGIN, TransportPutComposableIndexTemplateAction.TYPE, templateRequest, innerListener);
    }

    private static boolean hasIndexTemplate(ClusterState state, String templateName, long version) {
        var template = state.getMetadata().getProject().templatesV2().get(templateName);
        return template != null && Long.valueOf(version).equals(template.version());
    }

    /**
     * Ensures a given index name is valid for ML results by appending the 6-digit suffix if it is missing.
     *
     * @param indexName The index name to validate.
     * @return The validated index name, with the suffix added if it was missing.
     */
    public static String ensureValidResultsIndexName(String indexName) {
        // The results index name is either the original one provided or the original with a suffix appended.
        return has6DigitSuffix(indexName) ? indexName : indexName + FIRST_INDEX_SIX_DIGIT_SUFFIX;
    }

    /**
     * Checks if an index name ends with a 6-digit suffix (e.g., "-000001").
     *
     * @param indexName The name of the index to check.
     * @return {@code true} if the index name has a 6-digit suffix, {@code false} otherwise.
     */
    public static boolean has6DigitSuffix(String indexName) {
        String[] indexParts = indexName.split("-");
        String suffix = indexParts[indexParts.length - 1];
        return HAS_SIX_DIGIT_SUFFIX.test(suffix);
    }

    /**
     * Checks if an index name matches the pattern for the default ML anomalies indices (e.g., ".ml-anomalies-shared-000001").
     *
     * @param indexName The name of the index to check.
     * @return {@code true} if the index is a shared anomalies index, {@code false} otherwise.
     */
    public static boolean isAnomaliesSharedIndex(String indexName) {
        return IS_ANOMALIES_SHARED_INDEX.test(indexName);
    }

    /**
     * Checks if an index name matches the pattern for the ML anomalies state indices  (e.g., ".ml-state-000001").
     *
     * @param indexName The name of the index to check.
     * @return {@code true} if the index is an anomalies state index, {@code false} otherwise.
     */
    public static boolean isAnomaliesStateIndex(String indexName) {
        return IS_ANOMALIES_STATE_INDEX.test(indexName);
    }

    /**
     * Returns the latest index. Latest is the index with the highest
     * 6 digit suffix.
     * @param concreteIndices List of index names
     * @return The latest index by index name version suffix
     */
    public static String latestIndex(String[] concreteIndices) {
        return concreteIndices.length == 1
            ? concreteIndices[0]
            : Arrays.stream(concreteIndices).max(MlIndexAndAlias.INDEX_NAME_COMPARATOR).get();
    }

    /**
     * Sorts the given list of indices based on their 6 digit suffix.
     * @param indices List of index names
     */
    public static void sortIndices(List<String> indices) {
        indices.sort(INDEX_NAME_COMPARATOR);
    }

    /**
     * True if the version is read *and* write compatible not just read only compatible
     */
    public static boolean indexIsReadWriteCompatibleInV9(IndexVersion version) {
        return version.onOrAfter(IndexVersions.V_8_0_0);
    }

    /**
     * Returns the given index name without its 6 digit suffix.
     * @param index The index name to check
     * @return The base index name, without the 6 digit suffix.
     */
    public static String baseIndexName(String index) {
        return MlIndexAndAlias.has6DigitSuffix(index) ? index.substring(0, index.length() - FIRST_INDEX_SIX_DIGIT_SUFFIX.length()) : index;
    }

    /**
     * Returns an array of indices that match the given base index name.
     * @param baseIndexName         The base part of an index name, without the 6 digit suffix.
     * @param expressionResolver    The expression resolver
     * @param latestState           The latest cluster state
     * @return                      An array of matching indices.
     */
    public static String[] indicesMatchingBasename(
        String baseIndexName,
        IndexNameExpressionResolver expressionResolver,
        ClusterState latestState
    ) {
        return expressionResolver.concreteIndexNames(latestState, IndicesOptions.lenientExpandOpenHidden(), baseIndexName + "*");
    }

    /**
     * Strip any suffix from the index name and find any other indices
     * that match the base name. Then return the latest index from the
     * matching ones.
     *
     * @param index The index to check
     * @param expressionResolver The expression resolver
     * @param latestState The latest cluster state
     * @return The latest index that matches the base name of the given index
     */
    public static String latestIndexMatchingBaseName(
        String index,
        IndexNameExpressionResolver expressionResolver,
        ClusterState latestState
    ) {

        String baseIndexName = baseIndexName(index);

        var matching = indicesMatchingBasename(baseIndexName, expressionResolver, latestState);

        // We used to assert here if no matching indices could be found. However, when called _before_ a job is created it may be the case
        // that no .ml-anomalies-shared* indices yet exist
        if (matching.length == 0) {
            return index;
        }

        // Exclude indices that start with the same base name but are a different index
        // e.g. .ml-anomalies-foobar should not be included when the index name is
        // .ml-anomalies-foo
        String[] filtered = Arrays.stream(matching).filter(i -> {
            return i.equals(index) || (has6DigitSuffix(i) && i.length() == baseIndexName.length() + FIRST_INDEX_SIX_DIGIT_SUFFIX.length());
        }).toArray(String[]::new);

        return MlIndexAndAlias.latestIndex(filtered);
    }

    /**
     * Executes a rollover request. It handles {@link ResourceAlreadyExistsException} gracefully by treating it as a success
     * and returning the name of the existing index.
     *
     * @param client            The client to use for the rollover request.
     * @param rolloverRequest   The rollover request to execute.
     * @param listener          A listener that will be notified with the name of the new (or pre-existing) index on success,
     *                          or an exception on failure.
     */
    public static void rollover(Client client, RolloverRequest rolloverRequest, ActionListener<String> listener) {
        client.admin()
            .indices()
            .rolloverIndex(rolloverRequest, ActionListener.wrap(response -> listener.onResponse(response.getNewIndex()), e -> {
                if (e instanceof ResourceAlreadyExistsException alreadyExistsException) {
                    // The destination index already exists possibly because it has been rolled over already.
                    listener.onResponse(alreadyExistsException.getIndex().getName());
                } else {
                    listener.onFailure(e);
                }
            }));
    }

    /**
     * Generates a temporary rollover alias and a potential new index name based on a source index name.
     * This is a preparatory step for a rollover action. If the source index already has a 6-digit suffix,
     * the new index name will be null, allowing the rollover API to auto-increment the suffix.
     *
     * @param index The name of the index that is a candidate for rollover.
     * @return A {@link Tuple} where {@code v1} is the generated rollover alias and {@code v2} is the new index name
     * (or {@code null} if rollover can auto-determine it).
     */
    public static Tuple<String, String> createRolloverAliasAndNewIndexName(String index) {
        String indexName = Objects.requireNonNull(index);

        // Create an alias specifically for rolling over.
        // The ml-anomalies index has aliases for each job, any
        // of which could be used but that means one alias is
        // treated differently.
        // ROLLOVER_ALIAS_SUFFIX puts a `.` in the alias name to avoid any conflicts
        // as AD job Ids cannot start with `.`
        String rolloverAlias = indexName + ROLLOVER_ALIAS_SUFFIX;

        // If the index does not end in a digit then rollover does not know
        // what to name the new index so it must be specified in the request.
        // Otherwise leave null and rollover will calculate the new name
        String newIndexName = MlIndexAndAlias.has6DigitSuffix(index) ? null : indexName + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX;

        return new Tuple<>(rolloverAlias, newIndexName);
    }

    /**
     * Creates a pre-configured {@link IndicesAliasesRequestBuilder} with default timeouts.
     *
     * @param client The client to use for the request.
     * @return A new {@link IndicesAliasesRequestBuilder}.
     */
    public static IndicesAliasesRequestBuilder createIndicesAliasesRequestBuilder(Client client) {
        return client.admin().indices().prepareAliases(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS);
    }

    /**
     * Creates a hidden alias for an index, typically used as a rollover target.
     *
     * @param client      The client to use for the alias request.
     * @param indexName   The name of the index to which the alias will be added.
     * @param aliasName   The name of the alias to create.
     * @param listener    A listener that will be notified with the response.
     */
    public static void createAliasForRollover(
        Client client,
        String indexName,
        String aliasName,
        ActionListener<IndicesAliasesResponse> listener
    ) {
        logger.info("creating rollover [{}] alias for [{}]", aliasName, indexName);
        createIndicesAliasesRequestBuilder(client).addAliasAction(
            IndicesAliasesRequest.AliasActions.add().index(indexName).alias(aliasName).isHidden(true)
        ).execute(listener);
    }

    /**
     * Executes a prepared {@link IndicesAliasesRequestBuilder} and notifies the listener of the result.
     *
     * @param request  The prepared request builder containing alias actions.
     * @param listener A listener that will be notified with {@code true} on success.
     */
    public static void updateAliases(IndicesAliasesRequestBuilder request, ActionListener<Boolean> listener) {
        request.execute(listener.delegateFailure((l, response) -> l.onResponse(Boolean.TRUE)));
    }

    /**
     * Adds alias actions to a request builder to move the ML state write alias from an old index to a new one after a rollover.
     * This method is robust and will move the correct alias regardless of the current alias state on the old index.
     *
     * @param aliasRequestBuilder The request builder to add actions to.
     * @param newIndex            The new index to which the alias will be moved.
     * @param clusterState        The current cluster state, used to inspect existing aliases on the old index.
     * @param allStateIndices     A list of all current .ml-state indices
     * @return The modified {@link IndicesAliasesRequestBuilder}.
     */
    public static IndicesAliasesRequestBuilder addStateIndexRolloverAliasActions(
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        String newIndex,
        ClusterState clusterState,
        List<String> allStateIndices
    ) {
        allStateIndices.stream().filter(index -> clusterState.metadata().getProject().index(index) != null).forEach(index -> {
            // Remove the write alias from ALL state indices to handle any inconsistencies where it might exist on more than one.
            aliasRequestBuilder.addAliasAction(
                IndicesAliasesRequest.AliasActions.remove().indices(index).alias(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
            );
        });

        // Add the write alias to the latest state index
        aliasRequestBuilder.addAliasAction(
            IndicesAliasesRequest.AliasActions.add()
                .index(newIndex)
                .alias(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
                .isHidden(true)
                .writeIndex(true)
        );

        return aliasRequestBuilder;

    }

    private static Optional<String> findEarliestIndexWithAlias(Map<String, List<AliasMetadata>> aliasesMap, String targetAliasName) {
        return aliasesMap.entrySet()
            .stream()
            .filter(entry -> entry.getValue().stream().anyMatch(am -> am.alias().equals(targetAliasName)))
            .map(Map.Entry::getKey)
            .min(INDEX_NAME_COMPARATOR);
    }

    private static void addReadAliasesForResultsIndices(
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        String jobId,
        Map<String, List<AliasMetadata>> aliasesMap,
        List<String> allJobResultsIndices,
        String readAliasName
    ) {
        // Try to generate a sub list of indices to operate on where the first index in the list is the first one with the current
        // read alias. This is useful in trying to "heal" missing read aliases, without adding them on every possible index.
        int indexOfEarliestIndexWithAlias = findEarliestIndexWithAlias(aliasesMap, readAliasName).map(allJobResultsIndices::indexOf)
            .filter(i -> i >= 0)
            .orElse(0); // If the earliest index is not found in the list (which shouldn't happen), default to 0 to include all indices.

        aliasRequestBuilder.addAliasAction(
            IndicesAliasesRequest.AliasActions.add()
                .indices(allJobResultsIndices.subList(indexOfEarliestIndexWithAlias, allJobResultsIndices.size()).toArray(new String[0]))
                .alias(readAliasName)
                .isHidden(true)
                .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
        );

    }

    /**
     * Adds alias actions to a request builder to move ML job aliases from an old index to a new one after a rollover.
     * This includes moving the write alias and re-creating the filtered read aliases on the new index.
     *
     * @param aliasRequestBuilder       The request builder to add actions to.
     * @param newIndex                  The new index to which aliases will be moved.
     * @param clusterState              The current cluster state, used to inspect existing aliases on the old index.
     * @param currentJobResultsIndices  A list of all current .ml-anomalies indices
     * @return The modified {@link IndicesAliasesRequestBuilder}.
     */
    public static IndicesAliasesRequestBuilder addResultsIndexRolloverAliasActions(
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        String newIndex,
        ClusterState clusterState,
        List<String> currentJobResultsIndices
    ) {
        // Multiple jobs can share the same index, each job should have
        // a read and write alias that needs updating after the rollover
        var aliasesMap = clusterState.metadata().getProject().findAllAliases(currentJobResultsIndices.toArray(new String[0]));
        if (aliasesMap == null) {
            // This should not happen in practice, but we defend against it.
            return aliasRequestBuilder;
        }

        // Make sure to include the new index
        List<String> allJobResultsIndices = new ArrayList<>(currentJobResultsIndices);
        allJobResultsIndices.add(newIndex);
        MlIndexAndAlias.sortIndices(allJobResultsIndices);

        // Group all unique aliases by their job ID. This ensures each job is processed only once.
        aliasesMap.values()
            .stream()
            .flatMap(List::stream)
            .filter(alias -> isAnomaliesReadAlias(alias.alias()) || isAnomaliesWriteAlias(alias.alias()))
            .flatMap(alias -> AnomalyDetectorsIndex.jobIdFromAlias(alias.alias()).stream().map(jobId -> new Tuple<>(jobId, alias)))
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())))
            .forEach((jobId, jobAliases) -> {
                // For each job, ensure its aliases are correctly configured for the rollover.
                String writeAliasName = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
                String readAliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

                // 1. Move the write alias to the new index.
                moveWriteAlias(aliasRequestBuilder, newIndex, currentJobResultsIndices, writeAliasName);

                // 2. Ensure the read alias is correctly applied across the relevant indices.
                addReadAliasesForResultsIndices(aliasRequestBuilder, jobId, aliasesMap, allJobResultsIndices, readAliasName);
            });

        return aliasRequestBuilder;
    }

    private static void moveWriteAlias(
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        String newIndex,
        List<String> currentJobResultsIndices,
        String writeAliasName
    ) {
        // Remove the write alias from ALL job results indices to handle any inconsistencies where it might exist on more than one.
        aliasRequestBuilder.addAliasAction(
            IndicesAliasesRequest.AliasActions.remove().indices(currentJobResultsIndices.toArray(new String[0])).alias(writeAliasName)
        );
        // Add the write alias to the latest results index
        aliasRequestBuilder.addAliasAction(
            IndicesAliasesRequest.AliasActions.add().index(newIndex).alias(writeAliasName).isHidden(true).writeIndex(true)
        );
    }

    /**
     * Determines if an alias name is an ML anomalies write alias.
     *
     * @param aliasName The alias name to check.
     * @return {@code true} if the name matches the write alias pattern, {@code false} otherwise.
     */
    public static boolean isAnomaliesWriteAlias(String aliasName) {
        return aliasName.startsWith(AnomalyDetectorsIndexFields.RESULTS_INDEX_WRITE_PREFIX);
    }

    /**
     * Determines if an alias name is an ML anomalies read alias.
     *
     * @param aliasName The alias name to check.
     * @return {@code true} if the name matches the read alias pattern, {@code false} otherwise.
     */
    public static boolean isAnomaliesReadAlias(String aliasName) {
        if (aliasName.startsWith(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX) == false) {
            return false;
        }

        // See {@link AnomalyDetectorsIndex#jobResultsAliasedName}
        String jobIdPart = aliasName.substring(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX.length());
        // If this is a write alias it will start with a `.` character
        // which is not a valid job id.
        return MlStrings.isValidId(jobIdPart);
    }
}
