/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzerAction;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Manages synonyms performing operations on the system index
 */
public class SynonymsManagementAPIService {
    public static final String SYNONYMS_INDEX_NAME_PATTERN = ".synonyms-*";
    public static final String SYNONYMS_INDEX_CONCRETE_NAME = ".synonyms-1";
    public static final String SYNONYMS_ALIAS_NAME = ".synonyms";

    public static final String SYNONYMS_FEATURE_NAME = "synonyms";
    public static final String SYNONYMS_SET_FIELD = "synonyms_set";
    public static final String SYNONYMS_FIELD = SynonymRule.SYNONYMS_FIELD.getPreferredName();
    public static final String SYNONYM_RULE_ID_SEPARATOR = "|";
    public static final String SYNONYM_SETS_AGG_NAME = "synonym_sets_aggr";
    public static final int MAX_SYNONYMS_SETS = 10_000;
    public static final String SYNONYM_RULE_ID_FIELD = SynonymRule.ID_FIELD.getPreferredName();

    private final Client client;

    public static final String SYNONYMS_ORIGIN = "synonyms";
    public static final SystemIndexDescriptor SYNONYMS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(SYNONYMS_INDEX_NAME_PATTERN)
        .setDescription("Synonyms index for synonyms managed through APIs")
        .setPrimaryIndex(SYNONYMS_INDEX_CONCRETE_NAME)
        .setAliasName(SYNONYMS_ALIAS_NAME)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(SYNONYMS_ORIGIN)
        .build();

    public SynonymsManagementAPIService(Client client) {
        this.client = new OriginSettingClient(client, SYNONYMS_ORIGIN);
    }

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
            .addAggregation(
                new TermsAggregationBuilder(SYNONYM_SETS_AGG_NAME).field(SYNONYMS_SET_FIELD)
                    .order(BucketOrder.key(true))
                    .size(MAX_SYNONYMS_SETS)
            )
            .setPreference(Preference.LOCAL.type())
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Terms aggregation = searchResponse.getAggregations().get(SYNONYM_SETS_AGG_NAME);
                    List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
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
        client.prepareSearch(SYNONYMS_ALIAS_NAME)
            .setQuery(QueryBuilders.termQuery(SYNONYMS_SET_FIELD, synonymSetId))
            .setFrom(from)
            .setSize(size)
            .addSort("id", SortOrder.ASC)
            .setPreference(Preference.LOCAL.type())
            .setTrackTotalHits(true)
            .execute(new DelegatingIndexNotFoundActionListener<>(synonymSetId, listener, (l, searchResponse) -> {
                final long totalSynonymRules = searchResponse.getHits().getTotalHits().value;
                if (totalSynonymRules == 0) {
                    l.onFailure(new ResourceNotFoundException("Synonym set [" + synonymSetId + "] not found"));
                    return;
                }
                final SynonymRule[] synonymRules = Arrays.stream(searchResponse.getHits().getHits())
                    .map(hit -> sourceMapToSynonymRule(hit.getSourceAsMap()))
                    .toArray(SynonymRule[]::new);
                l.onResponse(new PagedResult<>(totalSynonymRules, synonymRules));
            }));
    }

    private static SynonymRule sourceMapToSynonymRule(Map<String, Object> docSourceAsMap) {
        return new SynonymRule((String) docSourceAsMap.get(SYNONYM_RULE_ID_FIELD), (String) docSourceAsMap.get(SYNONYMS_FIELD));
    }

    public void putSynonymsSet(
        String resourceName,
        SynonymRule[] synonymsSet,
        ActionListener<SynonymsReloadResult<UpdateSynonymsResultStatus>> listener
    ) {
        deleteSynonymsSetRules(resourceName, listener.delegateFailure((deleteByQueryResponseListener, bulkDeleteResponse) -> {
            boolean created = bulkDeleteResponse.getDeleted() == 0;
            final List<BulkItemResponse.Failure> bulkDeleteFailures = bulkDeleteResponse.getBulkFailures();
            if (bulkDeleteFailures.isEmpty() == false) {
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
                for (SynonymRule synonymRule : synonymsSet) {
                    bulkRequestBuilder.add(createSynonymRuleIndexRequest(resourceName, synonymRule));
                }
            } catch (IOException ex) {
                listener.onFailure(ex);
            }

            bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .execute(deleteByQueryResponseListener.delegateFailure((bulkInsertResponseListener, bulkInsertResponse) -> {
                    if (bulkInsertResponse.hasFailures()) {
                        bulkInsertResponseListener.onFailure(
                            new ElasticsearchException("Error updating synonyms: " + bulkInsertResponse.buildFailureMessage())
                        );
                        return;
                    }
                    UpdateSynonymsResultStatus updateSynonymsResultStatus = created
                        ? UpdateSynonymsResultStatus.CREATED
                        : UpdateSynonymsResultStatus.UPDATED;

                    reloadAnalyzers(bulkInsertResponseListener, updateSynonymsResultStatus);
                }));
        }));
    }

    public void putSynonymRule(
        String synonymsSetId,
        SynonymRule synonymRule,
        ActionListener<SynonymsReloadResult<UpdateSynonymsResultStatus>> listener
    ) {
        checkSynonymSetExists(synonymsSetId, listener.delegateFailure((l1, obj) -> {
            try {
                IndexRequest indexRequest = createSynonymRuleIndexRequest(synonymsSetId, synonymRule).setRefreshPolicy(
                    WriteRequest.RefreshPolicy.IMMEDIATE
                );
                client.index(indexRequest, l1.delegateFailure((l2, indexResponse) -> {
                    UpdateSynonymsResultStatus updateStatus = indexResponse.status() == RestStatus.CREATED
                        ? UpdateSynonymsResultStatus.CREATED
                        : UpdateSynonymsResultStatus.UPDATED;

                    reloadAnalyzers(l2, updateStatus);
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

    private static IndexRequest createSynonymRuleIndexRequest(String synonymsSetId, SynonymRule synonymRule) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            {
                builder.field(SYNONYMS_SET_FIELD, synonymsSetId);
                builder.field(SYNONYM_RULE_ID_FIELD, synonymRule.id());
                builder.field(SYNONYMS_FIELD, synonymRule.synonyms());
            }
            builder.endObject();

            return new IndexRequest(SYNONYMS_ALIAS_NAME).id(internalSynonymRuleId(synonymsSetId, synonymRule))
                .opType(DocWriteRequest.OpType.INDEX)
                .source(builder)
                .id(internalSynonymRuleId(synonymsSetId, synonymRule));
        }
    }

    private <T> void checkSynonymSetExists(String synonymsSetId, ActionListener<T> listener) {
        client.prepareSearch(SYNONYMS_ALIAS_NAME)
            .setQuery(QueryBuilders.termQuery(SYNONYMS_SET_FIELD, synonymsSetId))
            .setSize(1)
            .setPreference(Preference.LOCAL.type())
            .execute(new DelegatingIndexNotFoundActionListener<>(synonymsSetId, listener, (l, searchResponse) -> {
                if (searchResponse.getHits().getTotalHits().value == 0) {
                    l.onFailure(new ResourceNotFoundException("Synonym set [" + synonymsSetId + "] not found"));
                    return;
                }
                l.onResponse(null);
            }));
    }

    // Deletes a synonym set rules, using the supplied listener
    private void deleteSynonymsSetRules(String resourceName, ActionListener<BulkByScrollResponse> listener) {
        // Delete synonyms set if it existed previously. Avoid catching an index not found error by ignoring unavailable indices
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(SYNONYMS_ALIAS_NAME).setQuery(
            QueryBuilders.termQuery(SYNONYMS_SET_FIELD, resourceName)
        ).setRefresh(true).setIndicesOptions(IndicesOptions.fromOptions(true, true, false, false));

        client.execute(DeleteByQueryAction.INSTANCE, dbqRequest, listener);
    }

    public void deleteSynonymsSet(String resourceName, ActionListener<SynonymsReloadResult<AcknowledgedResponse>> listener) {
        deleteSynonymsSetRules(resourceName, listener.delegateFailure((l, bulkByScrollResponse) -> {
            if (bulkByScrollResponse.getDeleted() == 0) {
                // If nothing was deleted, synonym set did not exist
                l.onFailure(new ResourceNotFoundException("Synonym set [" + resourceName + "] not found"));
                return;
            }
            final List<BulkItemResponse.Failure> bulkFailures = bulkByScrollResponse.getBulkFailures();
            if (bulkFailures.isEmpty() == false) {
                l.onFailure(
                    new ElasticsearchException(
                        "Error deleting synonym set: "
                            + bulkFailures.stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.joining("\n"))
                    )
                );
                return;
            }

            reloadAnalyzers(l, AcknowledgedResponse.of(true));
        }));
    }

    private <T> void reloadAnalyzers(ActionListener<SynonymsReloadResult<T>> listener, T synonymsOperationResult) {
        // auto-reload all reloadable analyzers (currently only those that use updateable synonym or keyword_marker filters)
        // TODO: reload only those analyzers that use this synonymsSet
        ReloadAnalyzersRequest reloadAnalyzersRequest = new ReloadAnalyzersRequest("*");
        client.execute(
            ReloadAnalyzerAction.INSTANCE,
            reloadAnalyzersRequest,
            listener.delegateFailure((reloadResponseListener, reloadResponse) -> {
                reloadResponseListener.onResponse(new SynonymsReloadResult<>(synonymsOperationResult, reloadResponse));
            })
        );
    }

    // Retrieves the internal synonym rule ID to store it in the index. As the same synonym rule ID
    // can be used in different synonym sets, we prefix the ID with the synonym set to avoid collisions
    private static String internalSynonymRuleId(String synonymsSetId, SynonymRule synonymRule) {
        String synonymRuleId = synonymRule.id();
        if (synonymRuleId == null) {
            synonymRuleId = UUIDs.base64UUID();
        }
        final String id = synonymsSetId + SYNONYM_RULE_ID_SEPARATOR + synonymRuleId;
        return id;
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build();
    }

    public enum UpdateSynonymsResultStatus {
        CREATED,
        UPDATED
    }

    public record SynonymsReloadResult<T>(T synonymsOperationResult, ReloadAnalyzersResponse reloadAnalyzersResponse) {}

    // Listeners that checks failures for IndexNotFoundException, and transforms them in ResourceNotFoundException,
    // invoking onFailure on the delegate listener
    static class DelegatingIndexNotFoundActionListener<T, R> extends DelegatingActionListener<T, R> {

        private final BiConsumer<ActionListener<R>, T> bc;
        private final String resourceName;

        DelegatingIndexNotFoundActionListener(String resourceName, ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
            this.resourceName = resourceName;
        }

        @Override
        public void onResponse(T t) {
            bc.accept(delegate, t);
        }

        @Override
        public void onFailure(Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexNotFoundException) {
                delegate.onFailure(new ResourceNotFoundException("synonym set [" + resourceName + "] not found"));
                return;
            }
            delegate.onFailure(e);
        }
    }
}
