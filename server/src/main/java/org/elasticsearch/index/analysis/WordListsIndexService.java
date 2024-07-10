/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class WordListsIndexService {
    public static final String WORD_LISTS_FEATURE_NAME = "word_lists";
    public static final String WORD_LISTS_ORIGIN = "word_lists";

    private static final String WORD_LISTS_INDEX_NAME_PATTERN = ".word_lists-*";
    private static final int WORD_LISTS_INDEX_FORMAT = 1;
    private static final String WORD_LISTS_INDEX_CONCRETE_NAME = ".word_lists-" + WORD_LISTS_INDEX_FORMAT;
    private static final String WORD_LISTS_ALIAS_NAME = ".word_lists";
    private static final int WORD_LISTS_INDEX_MAPPINGS_VERSION = 1;
    private static final String WORD_LIST_ID_FIELD = "id";
    private static final String WORD_LIST_INDEX_FIELD = "index";
    private static final String WORD_LIST_NAME_FIELD = "name";
    private static final String WORD_LIST_VALUE_FIELD = "value";

    public static final SystemIndexDescriptor WORD_LISTS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(WORD_LISTS_INDEX_NAME_PATTERN)
        .setDescription("System index for word lists fetched from remote sources")
        .setPrimaryIndex(WORD_LISTS_INDEX_CONCRETE_NAME)
        .setAliasName(WORD_LISTS_ALIAS_NAME)
        .setIndexFormat(WORD_LISTS_INDEX_FORMAT)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(WORD_LISTS_ORIGIN)
        .build();

    private final Client client;

    public enum PutWordListResult {
        CREATED,
        UPDATED
    }

    public WordListsIndexService(Client client) {
        this.client = new OriginSettingClient(client, WORD_LISTS_ORIGIN);
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
                        builder.field(SystemIndexDescriptor.VERSION_META_KEY, WORD_LISTS_INDEX_MAPPINGS_VERSION);
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject(WORD_LIST_ID_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(WORD_LIST_INDEX_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(WORD_LIST_NAME_FIELD);
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject(WORD_LIST_VALUE_FIELD);
                        {
                            builder.field("type", "keyword");
                            builder.field("doc_values", false);
                            builder.field("index", false);
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
            throw new UncheckedIOException("Failed to build mappings for " + WORD_LISTS_INDEX_CONCRETE_NAME, e);
        }
    }

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), WORD_LISTS_INDEX_FORMAT)
            .build();
    }

    public void getWordListValue(String index, String wordListName, ActionListener<String> listener) {
        final String wordListId = generateWordListId(index, wordListName);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(WORD_LISTS_ALIAS_NAME)
            .setQuery(QueryBuilders.termQuery(WORD_LIST_ID_FIELD, wordListId))
            .setSize(1)
            .setPreference(Preference.LOCAL.type())
            .setTrackTotalHits(true);

        executeAsyncWithOrigin(
            client,
            WORD_LISTS_ORIGIN,
            TransportSearchAction.TYPE,
            searchRequestBuilder.request(),
            new DelegatingActionListener<>(listener) {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    final long wordListCount = searchResponse.getHits().getTotalHits().value;
                    if (wordListCount > 1) {
                        listener.onFailure(new IllegalStateException(wordListCount + " word lists have ID [" + wordListId + "]"));
                    } else if (wordListCount == 1) {
                        listener.onResponse((String) searchResponse.getHits().getHits()[0].getSourceAsMap().get(WORD_LIST_VALUE_FIELD));
                    } else {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof IndexNotFoundException) {
                        delegate.onResponse(null);
                        return;
                    }

                    super.onFailure(e);
                }
            });


//        client.prepareSearch(WORD_LISTS_ALIAS_NAME)
//            .setQuery(QueryBuilders.termQuery(WORD_LIST_ID_FIELD, wordListId))
//            .setSize(1)
//            .setPreference(Preference.LOCAL.type())
//            .setTrackTotalHits(true)
//            .execute(new DelegatingActionListener<>(listener) {
//                @Override
//                public void onResponse(SearchResponse searchResponse) {
//                    final long wordListCount = searchResponse.getHits().getTotalHits().value;
//                    if (wordListCount > 1) {
//                        listener.onFailure(new IllegalStateException(wordListCount + " word lists have ID [" + wordListId + "]"));
//                    } else if (wordListCount == 1) {
//                        listener.onResponse((String) searchResponse.getHits().getHits()[0].getSourceAsMap().get(WORD_LIST_VALUE_FIELD));
//                    } else {
//                        listener.onResponse(null);
//                    }
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    Throwable cause = ExceptionsHelper.unwrapCause(e);
//                    if (cause instanceof IndexNotFoundException) {
//                        delegate.onResponse(null);
//                        return;
//                    }
//
//                    super.onFailure(e);
//                }
//            });
    }

    public void putWordList(String index, String wordListName, String wordListValue, ActionListener<PutWordListResult> listener) {
        IndexRequest indexRequest = createWordListIndexRequest(index, wordListName, wordListValue).setRefreshPolicy(
            WriteRequest.RefreshPolicy.IMMEDIATE
        );

        executeAsyncWithOrigin(
            client,
            WORD_LISTS_ORIGIN,
            TransportIndexAction.TYPE,
            indexRequest,
            listener.delegateFailure((l, indexResponse) -> {
                PutWordListResult result = indexResponse.status() == RestStatus.CREATED
                    ? PutWordListResult.CREATED
                    : PutWordListResult.UPDATED;

                l.onResponse(result);
            })
        );

//        client.index(indexRequest, listener.delegateFailure((l, indexResponse) -> {
//            PutWordListResult result = indexResponse.status() == RestStatus.CREATED
//                ? PutWordListResult.CREATED
//                : PutWordListResult.UPDATED;
//
//            l.onResponse(result);
//        }));
    }

    private static String generateWordListId(String index, String wordListName) {
        return index + "_" + wordListName;
    }

    private static IndexRequest createWordListIndexRequest(String index, String wordListName, String wordListValue) {
        final String wordListId = generateWordListId(index, wordListName);
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field(WORD_LIST_ID_FIELD, wordListId);
                builder.field(WORD_LIST_INDEX_FIELD, index);
                builder.field(WORD_LIST_NAME_FIELD, wordListName);
                builder.field(WORD_LIST_VALUE_FIELD, wordListValue);
            }
            builder.endObject();

            return new IndexRequest(WORD_LISTS_ALIAS_NAME).id(wordListId).opType(DocWriteRequest.OpType.INDEX).source(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to build word list index request", e);
        }
    }

    // Copied from ClientHelper to avoid more refactoring for the moment. This class and all other customized filters should probably move
    // to the core plugin though.
    private static <Request, Response> void executeAsyncWithOrigin(
        ThreadContext threadContext,
        String origin,
        Request request,
        ActionListener<Response> listener,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(origin)) {
            consumer.accept(request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }

    private static <Request extends ActionRequest, Response extends ActionResponse> void executeAsyncWithOrigin(
        Client client,
        String origin,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), origin, request, listener, (r, l) -> client.execute(action, r, l));
    }
}
