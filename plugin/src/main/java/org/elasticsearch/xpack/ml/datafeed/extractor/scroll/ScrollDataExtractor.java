/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.ml.utils.DomainSplitFunction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * An implementation that extracts data from elasticsearch using search and scroll on a client.
 * It supports safe and responsive cancellation by continuing the scroll until a new timestamp
 * is seen.
 * Note that this class is NOT thread-safe.
 */
class ScrollDataExtractor implements DataExtractor {

    private static final Logger LOGGER = Loggers.getLogger(ScrollDataExtractor.class);
    private static final TimeValue SCROLL_TIMEOUT = new TimeValue(10, TimeUnit.MINUTES);

    private final Client client;
    private final ScrollDataExtractorContext context;
    private String scrollId;
    private boolean isCancelled;
    private boolean hasNext;
    private Long timestampOnCancel;

    ScrollDataExtractor(Client client, ScrollDataExtractorContext dataExtractorContext) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(dataExtractorContext);
        this.hasNext = true;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        LOGGER.trace("[{}] Data extractor received cancel request", context.jobId);
        isCancelled = true;
    }

    @Override
    public Optional<InputStream> next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Optional<InputStream> stream = scrollId == null ? Optional.ofNullable(initScroll()) : Optional.ofNullable(continueScroll());
        if (!stream.isPresent()) {
            hasNext = false;
        }
        return stream;
    }

    private InputStream initScroll() throws IOException {
        LOGGER.debug("[{}] Initializing scroll", context.jobId);
        SearchResponse searchResponse = executeSearchRequest(buildSearchRequest());
        return processSearchResponse(searchResponse);
    }

    protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        return searchRequestBuilder.get();
    }

    private SearchRequestBuilder buildSearchRequest() {
        SearchRequestBuilder searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(client)
                .setScroll(SCROLL_TIMEOUT)
                .addSort(context.extractedFields.timeField(), SortOrder.ASC)
                .setIndices(context.indexes)
                .setTypes(context.types)
                .setSize(context.scrollSize)
                .setQuery(ExtractorUtils.wrapInTimeRangeQuery(
                        context.query, context.extractedFields.timeField(), context.start, context.end));

        for (String docValueField : context.extractedFields.getDocValueFields()) {
            searchRequestBuilder.addDocValueField(docValueField);
        }
        String[] sourceFields = context.extractedFields.getSourceFields();
        if (sourceFields.length == 0) {
            searchRequestBuilder.setFetchSource(false);
            searchRequestBuilder.storedFields(StoredFieldsContext._NONE_);
        } else {
            searchRequestBuilder.setFetchSource(sourceFields, null);
        }
        context.scriptFields.forEach(f -> searchRequestBuilder.addScriptField(
                f.fieldName(), injectDomainSplit(f.script())));
        return searchRequestBuilder;
    }

    private Script injectDomainSplit(Script script) {
        String code = script.getIdOrCode();
        if (code.contains("domainSplit(") && script.getLang().equals("painless")) {
            String modifiedCode = DomainSplitFunction.function + code;
            Map<String, Object> modifiedParams = new HashMap<>(script.getParams().size()
                    + DomainSplitFunction.params.size());

            modifiedParams.putAll(script.getParams());
            modifiedParams.putAll(DomainSplitFunction.params);

            return new Script(script.getType(), script.getLang(), modifiedCode, modifiedParams);
        }
        return script;
    }

    private InputStream processSearchResponse(SearchResponse searchResponse) throws IOException {
        ExtractorUtils.checkSearchWasSuccessful(context.jobId, searchResponse);
        scrollId = searchResponse.getScrollId();
        if (searchResponse.getHits().getHits().length == 0) {
            hasNext = false;
            clearScroll(scrollId);
            return null;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (SearchHitToJsonProcessor hitProcessor = new SearchHitToJsonProcessor(context.extractedFields, outputStream)) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                if (isCancelled) {
                    Long timestamp = context.extractedFields.timeFieldValue(hit);
                    if (timestamp != null) {
                        if (timestampOnCancel == null) {
                            timestampOnCancel = timestamp;
                        } else if (timestamp != timestampOnCancel) {
                            hasNext = false;
                            clearScroll(scrollId);
                            break;
                        }
                    }
                }
                hitProcessor.process(hit);
            }
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private InputStream continueScroll() throws IOException {
        LOGGER.debug("[{}] Continuing scroll with id [{}]", context.jobId, scrollId);
        SearchResponse searchResponse = executeSearchScrollRequest(scrollId);
        return processSearchResponse(searchResponse);
    }

    protected SearchResponse executeSearchScrollRequest(String scrollId) {
        return SearchScrollAction.INSTANCE.newRequestBuilder(client)
                .setScroll(SCROLL_TIMEOUT)
                .setScrollId(scrollId)
                .get();
    }

    void clearScroll(String scrollId) {
        ClearScrollAction.INSTANCE.newRequestBuilder(client).addScrollId(scrollId).get();
    }
}
