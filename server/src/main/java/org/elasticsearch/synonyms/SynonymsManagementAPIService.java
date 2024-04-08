/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Manages synonyms performing operations on the system index
 */
public class SynonymsManagementAPIService {

    private static final Logger logger = LogManager.getLogger(SynonymsManagementAPIService.class);
    private static final String SYNONYMS_INDEX_NAME_PATTERN = ".synonyms-*";
    private static final int SYNONYMS_INDEX_FORMAT = 2;
    private static final String SYNONYMS_INDEX_CONCRETE_NAME = ".synonyms-" + SYNONYMS_INDEX_FORMAT;
    private static final String SYNONYMS_ALIAS_NAME = ".synonyms";
    public static final String SYNONYMS_FEATURE_NAME = "synonyms";
    // Stores the synonym set the rule belongs to
    public static final String SYNONYMS_SET_FIELD = "synonyms_set";
    // Stores the synonym rule
    public static final String SYNONYMS_FIELD = SynonymRule.SYNONYMS_FIELD.getPreferredName();
    // Field that stores either SYNONYM_RULE_OBJECT_TYPE or SYNONYM_SET_OBJECT_TYPE
    private static final String OBJECT_TYPE_FIELD = "type";
    // Identifies synonym rule objects stored in the index
    private static final String SYNONYM_RULE_OBJECT_TYPE = "synonym_rule";
    // Identifies synonym set objects stored in the index
    private static final String SYNONYM_SET_OBJECT_TYPE = "synonym_set";
    private static final String SYNONYM_RULE_ID_SEPARATOR = "|";
    public static final int MAX_SYNONYMS_SETS = 10_000;
    private static final String SYNONYM_RULE_ID_FIELD = SynonymRule.ID_FIELD.getPreferredName();
    private static final String SYNONYM_SETS_AGG_NAME = "synonym_sets_aggr";
    private static final int SYNONYMS_INDEX_MAPPINGS_VERSION = 1;

    private final Client client;

    public static final String SYNONYMS_ORIGIN = "synonyms";
    public static final SystemIndexDescriptor SYNONYMS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(SYNONYMS_INDEX_NAME_PATTERN)
        .setDescription("Synonyms index for synonyms managed through APIs")
        .setPrimaryIndex(SYNONYMS_INDEX_CONCRETE_NAME)
        .setAliasName(SYNONYMS_ALIAS_NAME)
        .setIndexFormat(SYNONYMS_INDEX_FORMAT)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(SYNONYMS_ORIGIN)
        .build();

    public SynonymsManagementAPIService(Client client) {
        this.client = new OriginSettingClient(client, SYNONYMS_ORIGIN);
    }

    /* The synonym index stores two object types:
    - Synonym rules:
        - SYNONYM_RULE_ID_FIELD contains the synonym rule ID
        - SYNONYMS_FIELD contains the synonyms
        - SYNONYMS_SET_FIELD contains the synonym set the rule belongs to
        - OBJECT_TYPE_FIELD contains SYNONYM_RULE_OBJECT_TYPE
        - The id is calculated using internalSynonymRuleId method
    - Synonym sets:
        - SYNONYMS_SET_FIELD contains the synonym set name
        - OBJECT_TYPE_FIELD contains SYNONYM_SET_OBJECT_TYPE
        - The id is the synonym set name
     */
    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject(SINGLE_MAPPING_NAME);
                {
                    builder.startObject("_meta");
                    {
                        builder.field("version", Version.CURRENT.toString());
                        builder.field(SystemIndexDescriptor.VERSION_META_KEY, SYNONYMS_INDEX_MAPPINGS_VERSION);
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject(SYNONYM_RULE_ID_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(SYNONYMS_FIELD);
                        {
                            builder.field("type", "match_only_text");
                        }
                        builder.endObject();
                        builder.startObject(SYNONYMS_SET_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(OBJECT_TYPE_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + SYNONYMS_INDEX_CONCRETE_NAME, e);
        }
    }

    public void getSynonymsSets(int from, int size, ActionListener<PagedResult<SynonymSetSummary>> listener) {
        client.prepareSearch(SYNONYMS_ALIAS_NAME)
            .setSize(0)
            // Retrieves aggregated synonym rules for each synonym set, excluding the synonym set object type
            .setQuery(QueryBuilders.termQuery(OBJECT_TYPE_FIELD, SYNONYM_RULE_OBJECT_TYPE))
            .addAggregation(
                new TermsAggregationBuilder(SYNONYM_SETS_AGG_NAME).field(SYNONYMS_SET_FIELD)
                    .order(BucketOrder.key(true))
                    .size(MAX_SYNONYMS_SETS)
            )
            .setPreference(Preference.LOCAL.type())
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Terms termsAggregation = searchResponse.getAggregations().get(SYNONYM_SETS_AGG_NAME);
                    List<? extends Terms.Bucket> buckets = termsAggregation.getBuckets();
                    SynonymSetSummary[] synonymSetSummaries = buckets.stream()
                        .skip(from)
                        .limit(size)
                        .map(bucket -> new SynonymSetSummary(bucket.getDocCount(), bucket.getKeyAsString()))
                        .toArray(SynonymSetSummary[]::new);

                    listener.onResponse(new PagedResult<>(buckets.size(), synonymSetSummaries));
                }

                @Override
                public void onFailure(Exception e) {
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof IndexNotFoundException) {
                        // If System index has not been created yet, no synonym sets have been stored
                        listener.onResponse(new PagedResult<>(0L, new SynonymSetSummary[0]));
                        return;
                    }

                    listener.onFailure(e);
                }
            });
    }

    public void getSynonymSetRules(String synonymSetId, int from, int size, ActionListener<PagedResult<SynonymRule>> listener) {
        // Retrieves synonym rules, excluding the synonym set object type
        client.prepareSearch(SYNONYMS_ALIAS_NAME)
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(SYNONYMS_SET_FIELD, synonymSetId))
                    .filter(QueryBuilders.termQuery(OBJECT_TYPE_FIELD, SYNONYM_RULE_OBJECT_TYPE))
            )
            .setFrom(from)
            .setSize(size)
            .addSort("id", SortOrder.ASC)
            .setPreference(Preference.LOCAL.type())
            .setTrackTotalHits(true)
            .execute(new DelegatingIndexNotFoundActionListener<>(synonymSetId, listener, (searchListener, searchResponse) -> {
                final long totalSynonymRules = searchResponse.getHits().getTotalHits().value;
                // If there are no rules, check that the synonym set actually exists to return the proper error
                if (totalSynonymRules == 0) {
                    checkSynonymSetExists(synonymSetId, searchListener.delegateFailure((existsListener, response) -> {
                        searchListener.onResponse(new PagedResult<>(0, new SynonymRule[0]));
                    }));
                    return;
                }
                final SynonymRule[] synonymRules = Arrays.stream(searchResponse.getHits().getHits())
                    .map(hit -> sourceMapToSynonymRule(hit.getSourceAsMap()))
                    .toArray(SynonymRule[]::new);
                searchListener.onResponse(new PagedResult<>(totalSynonymRules, synonymRules));
            }));
    }

    private static SynonymRule sourceMapToSynonymRule(Map<String, Object> docSourceAsMap) {
        return new SynonymRule((String) docSourceAsMap.get(SYNONYM_RULE_ID_FIELD), (String) docSourceAsMap.get(SYNONYMS_FIELD));
    }

    private static void logUniqueFailureMessagesWithIndices(List<BulkItemResponse.Failure> bulkFailures) {
        // check if logger is at least debug
        if (logger.isDebugEnabled() == false) {
            return;
        }
        Map<String, List<BulkItemResponse.Failure>> uniqueFailureMessages = bulkFailures.stream()
            .collect(Collectors.groupingBy(BulkItemResponse.Failure::getMessage));
        // log each unique failure with their associated indices and the first stacktrace
        uniqueFailureMessages.forEach((failureMessage, failures) -> {
            logger.debug(
                "Error updating synonyms: [{}], indices: [{}], stacktrace: [{}]",
                failureMessage,
                failures.stream().map(BulkItemResponse.Failure::getIndex).collect(Collectors.joining(",")),
                ExceptionsHelper.formatStackTrace(failures.get(0).getCause().getStackTrace())
            );
        });
    }

    public void putSynonymsSet(String synonymSetId, SynonymRule[] synonymsSet, ActionListener<SynonymsReloadResult> listener) {
        deleteSynonymsSetObjects(synonymSetId, listener.delegateFailure((deleteByQueryResponseListener, bulkDeleteResponse) -> {
            boolean created = bulkDeleteResponse.getDeleted() == 0;
            final List<BulkItemResponse.Failure> bulkDeleteFailures = bulkDeleteResponse.getBulkFailures();
            if (bulkDeleteFailures.isEmpty() == false) {
                logUniqueFailureMessagesWithIndices(bulkDeleteFailures);
                listener.onFailure(
                    new ElasticsearchException(
                        "Error updating synonyms: "
                            + bulkDeleteFailures.stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.joining("\n"))
                    )
                );
                return;
            }

            // Insert as bulk requests
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            try {
                // Insert synonym set object
                bulkRequestBuilder.add(createSynonymSetIndexRequest(synonymSetId));
                // Insert synonym rules
                for (SynonymRule synonymRule : synonymsSet) {
                    bulkRequestBuilder.add(createSynonymRuleIndexRequest(synonymSetId, synonymRule));
                }
            } catch (IOException ex) {
                listener.onFailure(ex);
            }

            bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .execute(deleteByQueryResponseListener.delegateFailure((bulkInsertResponseListener, bulkInsertResponse) -> {
                    if (bulkInsertResponse.hasFailures()) {
                        logUniqueFailureMessagesWithIndices(
                            Arrays.stream(bulkInsertResponse.getItems())
                                .filter(BulkItemResponse::isFailed)
                                .map(BulkItemResponse::getFailure)
                                .collect(Collectors.toList())
                        );
                        bulkInsertResponseListener.onFailure(
                            new ElasticsearchException("Error updating synonyms: " + bulkInsertResponse.buildFailureMessage())
                        );
                        return;
                    }
                    UpdateSynonymsResultStatus updateSynonymsResultStatus = created
                        ? UpdateSynonymsResultStatus.CREATED
                        : UpdateSynonymsResultStatus.UPDATED;

                    reloadAnalyzers(synonymSetId, false, bulkInsertResponseListener, updateSynonymsResultStatus);
                }));
        }));
    }

    public void putSynonymRule(String synonymsSetId, SynonymRule synonymRule, ActionListener<SynonymsReloadResult> listener) {
        checkSynonymSetExists(synonymsSetId, listener.delegateFailure((l1, obj) -> {
            try {
                IndexRequest indexRequest = createSynonymRuleIndexRequest(synonymsSetId, synonymRule).setRefreshPolicy(
                    WriteRequest.RefreshPolicy.IMMEDIATE
                );
                client.index(indexRequest, l1.delegateFailure((l2, indexResponse) -> {
                    UpdateSynonymsResultStatus updateStatus = indexResponse.status() == RestStatus.CREATED
                        ? UpdateSynonymsResultStatus.CREATED
                        : UpdateSynonymsResultStatus.UPDATED;

                    reloadAnalyzers(synonymsSetId, false, l2, updateStatus);
                }));
            } catch (IOException e) {
                l1.onFailure(e);
            }
        }));
    }

    public void getSynonymRule(String synonymSetId, String synonymRuleId, ActionListener<SynonymRule> listener) {
        checkSynonymSetExists(
            synonymSetId,
            listener.delegateFailure(
                (l1, obj) -> client.prepareGet(SYNONYMS_ALIAS_NAME, internalSynonymRuleId(synonymSetId, synonymRuleId))
                    .execute(l1.delegateFailure((l2, getResponse) -> {
                        if (getResponse.isExists() == false) {
                            l2.onFailure(new ResourceNotFoundException("synonym rule [" + synonymRuleId + "] not found"));
                            return;
                        }
                        l2.onResponse(sourceMapToSynonymRule(getResponse.getSourceAsMap()));
                    }))
            )
        );
    }

    public void deleteSynonymRule(String synonymsSetId, String synonymRuleId, ActionListener<SynonymsReloadResult> listener) {
        client.prepareDelete(SYNONYMS_ALIAS_NAME, internalSynonymRuleId(synonymsSetId, synonymRuleId))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(new DelegatingIndexNotFoundActionListener<>(synonymsSetId, listener, (l, deleteResponse) -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    // When not found, check whether it's the synonym set not existing
                    checkSynonymSetExists(
                        synonymsSetId,
                        l.delegateFailure(
                            (checkListener, obj) -> checkListener.onFailure(
                                new ResourceNotFoundException(
                                    "synonym rule [" + synonymRuleId + "] not found on synonyms set [" + synonymsSetId + "]"
                                )
                            )
                        )
                    );
                    return;
                }

                reloadAnalyzers(synonymsSetId, false, listener, UpdateSynonymsResultStatus.DELETED);
            }));
    }

    private static IndexRequest createSynonymRuleIndexRequest(String synonymsSetId, SynonymRule synonymRule) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            {
                builder.field(SYNONYMS_SET_FIELD, synonymsSetId);
                builder.field(SYNONYM_RULE_ID_FIELD, synonymRule.id());
                builder.field(SYNONYMS_FIELD, synonymRule.synonyms());
                builder.field(OBJECT_TYPE_FIELD, SYNONYM_RULE_OBJECT_TYPE);
            }
            builder.endObject();

            return new IndexRequest(SYNONYMS_ALIAS_NAME).id(internalSynonymRuleId(synonymsSetId, synonymRule.id()))
                .opType(DocWriteRequest.OpType.INDEX)
                .source(builder);
        }
    }

    private static IndexRequest createSynonymSetIndexRequest(String synonymsSetId) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            {
                builder.field(SYNONYMS_SET_FIELD, synonymsSetId);
                builder.field(OBJECT_TYPE_FIELD, SYNONYM_SET_OBJECT_TYPE);
            }
            builder.endObject();

            return new IndexRequest(SYNONYMS_ALIAS_NAME).id(synonymsSetId).opType(DocWriteRequest.OpType.INDEX).source(builder);
        }
    }

    private <T> void checkSynonymSetExists(String synonymsSetId, ActionListener<T> listener) {
        // Get the document with the synonym set ID
        client.prepareGet(SYNONYMS_ALIAS_NAME, synonymsSetId)
            .execute(new DelegatingIndexNotFoundActionListener<>(synonymsSetId, listener, (l, getResponse) -> {
                if (getResponse.isExists() == false) {
                    l.onFailure(new ResourceNotFoundException("synonyms set [" + synonymsSetId + "] not found"));
                    return;
                }
                l.onResponse(null);
            }));
    }

    // Deletes a synonym set rules, using the supplied listener
    private void deleteSynonymsSetObjects(String synonymSetId, ActionListener<BulkByScrollResponse> listener) {
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(SYNONYMS_ALIAS_NAME).setQuery(
            QueryBuilders.termQuery(SYNONYMS_SET_FIELD, synonymSetId)
        ).setRefresh(true).setIndicesOptions(IndicesOptions.fromOptions(true, true, false, false));

        client.execute(DeleteByQueryAction.INSTANCE, dbqRequest, listener);
    }

    public void deleteSynonymsSet(String synonymSetId, ActionListener<AcknowledgedResponse> listener) {

        // Previews reloading the resource to understand its usage on indices
        reloadAnalyzers(synonymSetId, true, listener.delegateFailure((reloadListener, reloadResult) -> {
            Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadDetails = reloadResult.reloadAnalyzersResponse.getReloadDetails();
            if (reloadDetails.isEmpty() == false) {
                Set<String> indices = reloadDetails.entrySet()
                    .stream()
                    .map(entry -> entry.getValue().getIndexName())
                    .collect(Collectors.toSet());
                reloadListener.onFailure(
                    new IllegalArgumentException(
                        "synonyms set ["
                            + synonymSetId
                            + "] cannot be deleted as it is used in the following indices: "
                            + String.join(", ", indices)
                    )
                );
                return;
            }

            deleteSynonymsSetObjects(synonymSetId, listener.delegateFailure((deleteObjectsListener, bulkByScrollResponse) -> {
                if (bulkByScrollResponse.getDeleted() == 0) {
                    // If nothing was deleted, synonym set did not exist
                    deleteObjectsListener.onFailure(new ResourceNotFoundException("synonyms set [" + synonymSetId + "] not found"));
                    return;
                }
                final List<BulkItemResponse.Failure> bulkFailures = bulkByScrollResponse.getBulkFailures();
                if (bulkFailures.isEmpty() == false) {
                    deleteObjectsListener.onFailure(
                        new InvalidParameterException(
                            "Error deleting synonyms set: "
                                + bulkFailures.stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.joining("\n"))
                        )
                    );
                    return;
                }

                deleteObjectsListener.onResponse(AcknowledgedResponse.of(true));
            }));
        }), null);
    }

    private <T> void reloadAnalyzers(
        String synonymSetId,
        boolean preview,
        ActionListener<SynonymsReloadResult> listener,
        UpdateSynonymsResultStatus synonymsOperationResult
    ) {
        // auto-reload all reloadable analyzers (currently only those that use updateable synonym or keyword_marker filters)
        ReloadAnalyzersRequest reloadAnalyzersRequest = new ReloadAnalyzersRequest(synonymSetId, preview, "*");
        client.execute(
            TransportReloadAnalyzersAction.TYPE,
            reloadAnalyzersRequest,
            listener.safeMap(reloadResponse -> new SynonymsReloadResult(synonymsOperationResult, reloadResponse))
        );
    }

    // Retrieves the internal synonym rule ID to store it in the index. As the same synonym rule ID
    // can be used in different synonym sets, we prefix the ID with the synonym set to avoid collisions
    private static String internalSynonymRuleId(String synonymsSetId, String synonymRuleId) {
        return synonymsSetId + SYNONYM_RULE_ID_SEPARATOR + synonymRuleId;
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), SYNONYMS_INDEX_FORMAT)
            .build();
    }

    public enum UpdateSynonymsResultStatus {
        CREATED,
        UPDATED,
        DELETED
    }

    public record SynonymsReloadResult(
        UpdateSynonymsResultStatus synonymsOperationResult,
        ReloadAnalyzersResponse reloadAnalyzersResponse
    ) {}

    // Listeners that checks failures for IndexNotFoundException, and transforms them in ResourceNotFoundException,
    // invoking onFailure on the delegate listener
    static class DelegatingIndexNotFoundActionListener<T, R> extends DelegatingActionListener<T, R> {

        private final BiConsumer<ActionListener<R>, T> bc;
        private final String synonymSetId;

        DelegatingIndexNotFoundActionListener(String synonymSetId, ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
            this.synonymSetId = synonymSetId;
        }

        @Override
        public void onResponse(T t) {
            bc.accept(delegate, t);
        }

        @Override
        public void onFailure(Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexNotFoundException) {
                delegate.onFailure(new ResourceNotFoundException("synonyms set [" + synonymSetId + "] not found"));
                return;
            }
            delegate.onFailure(e);
        }
    }
}
