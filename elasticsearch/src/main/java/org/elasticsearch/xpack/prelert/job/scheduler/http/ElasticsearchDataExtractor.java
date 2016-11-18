/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchDataExtractor implements DataExtractor {

    private static final String CLEAR_SCROLL_TEMPLATE = "{\"scroll_id\":[\"%s\"]}";
    private static final Pattern TOTAL_HITS_PATTERN = Pattern.compile("\"hits\":\\{.*?\"total\":(.*?),");
    private static final Pattern EARLIEST_TIME_PATTERN = Pattern.compile("\"earliestTime\":\\{.*?\"value\":(.*?),");
    private static final Pattern LATEST_TIME_PATTERN = Pattern.compile("\"latestTime\":\\{.*?\"value\":(.*?),");
    private static final Pattern INDEX_PATTERN = Pattern.compile("\"_index\":\"(.*?)\"");
    private static final Pattern NUMBER_OF_SHARDS_PATTERN = Pattern.compile("\"number_of_shards\":\"(.*?)\"");
    private static final long CHUNK_THRESHOLD_MS = 3600000;
    private static final long MIN_CHUNK_SIZE_MS = 10000L;

    private final HttpRequester httpRequester;
    private final ElasticsearchUrlBuilder urlBuilder;
    private final ElasticsearchQueryBuilder queryBuilder;
    private final int scrollSize;
    private final ScrollState scrollState;
    private volatile long currentStartTime;
    private volatile long currentEndTime;
    private volatile long endTime;
    private volatile boolean isFirstSearch;
    private volatile boolean isCancelled;

    /**
     * The interval of each scroll search. Will be null when search is not chunked.
     */
    private volatile Long m_Chunk;

    private volatile Logger m_Logger;

    public ElasticsearchDataExtractor(HttpRequester httpRequester, ElasticsearchUrlBuilder urlBuilder,
            ElasticsearchQueryBuilder queryBuilder, int scrollSize) {
        this.httpRequester = Objects.requireNonNull(httpRequester);
        this.urlBuilder = Objects.requireNonNull(urlBuilder);
        this.scrollSize = scrollSize;
        this.queryBuilder = Objects.requireNonNull(queryBuilder);
        scrollState =  queryBuilder.isAggregated() ? ScrollState.createAggregated() : ScrollState.createDefault();
        isFirstSearch = true;
    }

    @Override
    public void newSearch(long startEpochMs, long endEpochMs, Logger logger) throws IOException {
        m_Logger = logger;
        m_Logger.info("Requesting data from '" + urlBuilder.getBaseUrl() + "' within [" + startEpochMs + ", " + endEpochMs + ")");
        scrollState.reset();
        currentStartTime = startEpochMs;
        currentEndTime = endEpochMs;
        endTime = endEpochMs;
        isCancelled = false;
        if (endEpochMs - startEpochMs > CHUNK_THRESHOLD_MS) {
            setUpChunkedSearch();
        }
        if (isFirstSearch) {
            queryBuilder.logQueryInfo(m_Logger);
            isFirstSearch = false;
        }
    }

    private void setUpChunkedSearch() throws IOException {
        m_Chunk = null;
        String url = urlBuilder.buildSearchSizeOneUrl();
        String response = requestAndGetStringResponse(url, queryBuilder.createDataSummaryQuery(currentStartTime, endTime));
        long totalHits = matchLong(response, TOTAL_HITS_PATTERN);
        if (totalHits > 0) {
            // Aggregation value may be a double
            currentStartTime = (long) matchDouble(response, EARLIEST_TIME_PATTERN);
            long latestTime = (long) matchDouble(response, LATEST_TIME_PATTERN);
            long dataTimeSpread = latestTime - currentStartTime;
            String index = matchString(response, INDEX_PATTERN);
            long shards = readNumberOfShards(index);
            m_Chunk = Math.max(MIN_CHUNK_SIZE_MS, (shards * scrollSize * dataTimeSpread) / totalHits);
            currentEndTime = currentStartTime + m_Chunk;
            m_Logger.debug("Chunked search configured:  totalHits = " + totalHits
                    + ", dataTimeSpread = " + dataTimeSpread + " ms, chunk size = " + m_Chunk
                    + " ms");
        } else {
            currentStartTime = endTime;
        }
    }

    private String requestAndGetStringResponse(String url, String body) throws IOException {
        m_Logger.trace("url ={}, body={}", url, body);
        HttpResponse response = httpRequester.get(url, body);
        if (response.getResponseCode() != HttpResponse.OK_STATUS) {
            throw new IOException("Request '" + url + "' failed with status code: "
                    + response.getResponseCode() + ". Response was:\n" + response.getResponseAsString());
        }
        return response.getResponseAsString();
    }

    private static long matchLong(String response, Pattern pattern) throws IOException {
        String match = matchString(response, pattern);
        try {
            return Long.parseLong(match);
        } catch (NumberFormatException e) {
            throw new IOException("Failed to parse long from pattern '" + pattern + "'. Response was:\n" + response, e);
        }
    }

    private static double matchDouble(String response, Pattern pattern) throws IOException {
        String match = matchString(response, pattern);
        try {
            return Double.parseDouble(match);
        } catch (NumberFormatException e) {
            throw new IOException("Failed to parse double from pattern '" + pattern + "'. Response was:\n" + response, e);
        }
    }

    private static String matchString(String response, Pattern pattern) throws IOException {
        Matcher matcher = pattern.matcher(response);
        if (!matcher.find()) {
            throw new IOException("Failed to parse string from pattern '" + pattern + "'. Response was:\n" + response);
        }
        return matcher.group(1);
    }

    private long readNumberOfShards(String index) throws IOException {
        String url = urlBuilder.buildIndexSettingsUrl(index);
        String response = requestAndGetStringResponse(url, null);
        return matchLong(response, NUMBER_OF_SHARDS_PATTERN);
    }

    @Override
    public void clear() {
        scrollState.reset();
    }

    @Override
    public Optional<InputStream> next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            return getNextStream();
        } catch (IOException e) {
            m_Logger.error("An error occurred during requesting data from: " + urlBuilder.getBaseUrl(), e);
            scrollState.forceComplete();
            throw e;
        }
    }

    private Optional<InputStream> getNextStream() throws IOException {
        while (hasNext()) {
            boolean isNewScroll = scrollState.getScrollId() == null || scrollState.isComplete();
            InputStream stream = isNewScroll ? initScroll() : continueScroll();
            stream = scrollState.updateFromStream(stream);
            if (scrollState.isComplete()) {
                clearScroll();
                advanceTime();

                // If it was a new scroll it means it returned 0 hits. If we are doing
                // a chunked search, we reconfigure the search in order to jump to the next
                // time interval where there are data.
                if (isNewScroll && hasNext() && !isCancelled && m_Chunk != null) {
                    setUpChunkedSearch();
                }
            } else {
                return Optional.of(stream);
            }
        }
        return Optional.empty();
    }

    private void clearScroll() {
        if (scrollState.getScrollId() == null) {
            return;
        }

        String url = urlBuilder.buildClearScrollUrl();
        try {
            HttpResponse response = httpRequester.delete(url, String.format(Locale.ROOT, CLEAR_SCROLL_TEMPLATE, scrollState.getScrollId()));

            // This is necessary to ensure the response stream has been consumed entirely.
            // Failing to do this can cause a lot of issues with Elasticsearch when
            // scheduled jobs are running concurrently.
            response.getResponseAsString();
        } catch (IOException e) {
            m_Logger.error("An error ocurred during clearing scroll context", e);
        }
        scrollState.clearScrollId();
    }

    @Override
    public boolean hasNext() {
        return !scrollState.isComplete() || (!isCancelled && currentStartTime < endTime);
    }

    private InputStream initScroll() throws IOException {
        String url = buildInitScrollUrl();
        String searchBody = queryBuilder.createSearchBody(currentStartTime, currentEndTime);
        m_Logger.trace("About to submit body " + searchBody + " to URL " + url);
        HttpResponse response = httpRequester.get(url, searchBody);
        if (response.getResponseCode() != HttpResponse.OK_STATUS) {
            throw new IOException("Request '" + url + "' failed with status code: "
                    + response.getResponseCode() + ". Response was:\n" + response.getResponseAsString());
        }
        return response.getStream();
    }

    private void advanceTime() {
        currentStartTime = currentEndTime;
        currentEndTime = m_Chunk == null ? endTime : Math.min(currentStartTime + m_Chunk, endTime);
    }

    private String buildInitScrollUrl() throws IOException {
        // With aggregations we don't want any hits returned for the raw data,
        // just the aggregations
        int size = queryBuilder.isAggregated() ? 0 : scrollSize;
        return urlBuilder.buildInitScrollUrl(size);
    }

    private InputStream continueScroll() throws IOException {
        // Aggregations never need a continuation
        if (!queryBuilder.isAggregated()) {
            String url = urlBuilder.buildContinueScrollUrl();
            HttpResponse response = httpRequester.get(url, scrollState.getScrollId());
            if (response.getResponseCode() == HttpResponse.OK_STATUS) {
                return response.getStream();
            }
            throw new IOException("Request '"  + url + "' with scroll id '"
                    + scrollState.getScrollId() + "' failed with status code: "
                    + response.getResponseCode() + ". Response was:\n"
                    + response.getResponseAsString());
        }
        return null;
    }

    @Override
    public void cancel() {
        isCancelled = true;
    }
}
