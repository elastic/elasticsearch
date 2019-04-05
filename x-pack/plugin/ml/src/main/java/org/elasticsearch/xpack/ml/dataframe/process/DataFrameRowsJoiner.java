/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

class DataFrameRowsJoiner implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameRowsJoiner.class);

    private static final int RESULTS_BATCH_SIZE = 1000;

    private final String analyticsId;
    private final Client client;
    private final DataFrameDataExtractor dataExtractor;
    private List<DataFrameDataExtractor.Row> currentDataFrameRows;
    private int currentDataFrameRowsIndex;
    private LinkedList<RowResults> currentResults;
    private boolean failed;

    DataFrameRowsJoiner(String analyticsId, Client client, DataFrameDataExtractor dataExtractor) {
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.client = Objects.requireNonNull(client);
        this.dataExtractor = Objects.requireNonNull(dataExtractor);
        this.currentResults = new LinkedList<>();
    }

    void processRowResults(RowResults rowResults) {
        if (failed) {
            // If we are in failed state we drop the results but we let the processor
            // parse the output
            return;
        }

        try {
            addResultAndJoinIfEndOfBatch(rowResults);
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Failed to join results", analyticsId), e);
            failed = true;
        }
    }

    private void addResultAndJoinIfEndOfBatch(RowResults rowResults) {
        currentResults.add(rowResults);
        if (currentResults.size() == RESULTS_BATCH_SIZE) {
            joinCurrentResults();
        }
    }

    private Optional<List<DataFrameDataExtractor.Row>> getNextDataRowsBatch() {
        try {
            return dataExtractor.next();
        } catch (IOException e) {
            // TODO Implement recovery strategy or better error reporting
            LOGGER.error("Error reading next batch of data frame rows", e);
            return Optional.empty();
        }
    }

    private void joinCurrentResults() {
        BulkRequest bulkRequest = new BulkRequest();
        while (currentResults.isEmpty() == false) {
            Optional<DataFrameDataExtractor.Row> row = findNextUnskippedRow();
            if (row.isPresent() == false) {
                continue;
            }

            RowResults result = currentResults.pop();
            checkChecksumsMatch(row.get(), result);

            SearchHit hit = row.get().getHit();
            Map<String, Object> source = new LinkedHashMap(hit.getSourceAsMap());
            source.putAll(result.getResults());
            new IndexRequest(hit.getIndex());
            IndexRequest indexRequest = new IndexRequest(hit.getIndex());
            indexRequest.id(hit.getId());
            indexRequest.source(source);
            indexRequest.opType(DocWriteRequest.OpType.INDEX);
            bulkRequest.add(indexRequest);
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse =
                ClientHelper.executeWithHeaders(dataExtractor.getHeaders(),
                    ClientHelper.ML_ORIGIN,
                    client,
                    () -> client.execute(BulkAction.INSTANCE, bulkRequest).actionGet());
            if (bulkResponse.hasFailures()) {
                LOGGER.error("Failures while writing data frame");
                // TODO Better error handling
            }
        }
        currentResults = new LinkedList<>();
    }

    private Optional<DataFrameDataExtractor.Row> findNextUnskippedRow() {
        advanceToNextBatchIfNecessary();
        DataFrameDataExtractor.Row row = null;
        while ((row == null || row.shouldSkip()) && currentDataFrameRowsIndex < currentDataFrameRows.size()) {
            row = currentDataFrameRows.get(currentDataFrameRowsIndex++);
        }
        return (row == null || row.shouldSkip()) ? Optional.empty() : Optional.of(row);
    }

    private void advanceToNextBatchIfNecessary() {
        if (currentDataFrameRows == null || currentDataFrameRowsIndex >= currentDataFrameRows.size()) {
            currentDataFrameRows = getNextDataRowsBatch().orElse(Collections.emptyList());
            if (currentDataFrameRows.isEmpty()) {
                throw ExceptionsHelper.serverError("No more data frame rows could be found while joining results");
            }
            currentDataFrameRowsIndex = 0;
        }
    }

    private void checkChecksumsMatch(DataFrameDataExtractor.Row row, RowResults result) {
        if (row.getChecksum() != result.getChecksum()) {
            String msg = "Detected checksum mismatch for document with id [" + row.getHit().getId() + "]; ";
            msg += "expected [" + row.getChecksum() + "] but result had [" + result.getChecksum() + "]; ";
            msg += "this implies the data frame index [" + row.getHit().getIndex() + "] was modified while the analysis was running. ";
            msg += "We rely on this index being immutable during a running analysis and so the results will be unreliable.";
            throw new RuntimeException(msg);
            // TODO Communicate this error to the user as effectively the analytics have failed (e.g. FAILED state, audit error, etc.)
        }
    }

    @Override
    public void close() {
        try {
            joinCurrentResults();
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Failed to join results", analyticsId), e);
            failed = true;
        } finally {
            try {
                consumeDataExtractor();
            } catch (Exception e) {
                LOGGER.error(new ParameterizedMessage("[{}] Failed to consume data extractor", analyticsId), e);
            }
        }
    }

    private void consumeDataExtractor() throws IOException {
        dataExtractor.cancel();
        while (dataExtractor.hasNext()) {
            dataExtractor.next();
        }
    }
}
