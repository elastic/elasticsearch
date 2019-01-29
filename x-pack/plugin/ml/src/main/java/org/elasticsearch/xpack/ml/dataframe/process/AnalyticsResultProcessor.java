/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AnalyticsResultProcessor {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsResultProcessor.class);

    private final Client client;
    private final DataFrameDataExtractor dataExtractor;
    private List<DataFrameDataExtractor.Row> currentDataFrameRows;
    private List<AnalyticsResult> currentResults;
    private final CountDownLatch completionLatch = new CountDownLatch(1);

    public AnalyticsResultProcessor(Client client, DataFrameDataExtractor dataExtractor) {
        this.client = Objects.requireNonNull(client);
        this.dataExtractor = Objects.requireNonNull(dataExtractor);
    }

    public void awaitForCompletion() {
        try {
            if (completionLatch.await(30, TimeUnit.MINUTES) == false) {
                LOGGER.warn("Timeout waiting for results processor to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Interrupted waiting for results processor to complete");
        }
    }

    public void process(AnalyticsProcess process) {

        try {
            Iterator<AnalyticsResult> iterator = process.readAnalyticsResults();
            while (iterator.hasNext()) {
                try {
                    AnalyticsResult result = iterator.next();
                    if (dataExtractor.hasNext() == false) {
                        return;
                    }
                    if (currentDataFrameRows == null) {
                        Optional<List<DataFrameDataExtractor.Row>> nextBatch = dataExtractor.next();
                        if (nextBatch.isPresent() == false) {
                            return;
                        }
                        currentDataFrameRows = nextBatch.get();
                        currentResults = new ArrayList<>(currentDataFrameRows.size());
                    }
                    currentResults.add(result);
                    if (currentResults.size() == currentDataFrameRows.size()) {
                        joinCurrentResults();
                        currentDataFrameRows = null;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error processing data frame analytics result", e);
                }

            }
        } catch (Exception e) {
            LOGGER.error("Error parsing data frame analytics output", e);
        } finally {
            completionLatch.countDown();
            process.consumeAndCloseOutputStream();
        }
    }

    private void joinCurrentResults() {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < currentDataFrameRows.size(); i++) {
            DataFrameDataExtractor.Row row = currentDataFrameRows.get(i);
            if (row.shouldSkip()) {
                continue;
            }
            AnalyticsResult result = currentResults.get(i);
            checkChecksumsMatch(row, result);

            SearchHit hit = row.getHit();
            Map<String, Object> source = new LinkedHashMap(hit.getSourceAsMap());
            source.putAll(result.getResults());
            IndexRequest indexRequest = new IndexRequest(hit.getIndex(), hit.getType(), hit.getId());
            indexRequest.source(source);
            indexRequest.opType(DocWriteRequest.OpType.INDEX);
            bulkRequest.add(indexRequest);
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = client.execute(BulkAction.INSTANCE, bulkRequest).actionGet();
            if (bulkResponse.hasFailures()) {
                LOGGER.error("Failures while writing data frame");
                // TODO Better error handling
            }
        }
    }

    private void checkChecksumsMatch(DataFrameDataExtractor.Row row, AnalyticsResult result) {
        if (row.getChecksum() != result.getChecksum()) {
            String msg = "Detected checksum mismatch for document with id [" + row.getHit().getId() + "]; ";
            msg += "expected [" + row.getChecksum() + "] but result had [" + result.getChecksum() + "]; ";
            msg += "this implies the data frame index [" + row.getHit().getIndex() + "] was modified while the analysis was running. ";
            msg += "We rely on this index being immutable during a running analysis and so the results will be unreliable.";
            throw new IllegalStateException(msg);
        }
    }
}
