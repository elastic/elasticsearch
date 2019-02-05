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
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class DataFrameRowsJoiner {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameRowsJoiner.class);

    private final String analyticsId;
    private final Client client;
    private final DataFrameDataExtractor dataExtractor;
    private List<DataFrameDataExtractor.Row> currentDataFrameRows;
    private List<RowResults> currentResults;
    private boolean failed;

    public DataFrameRowsJoiner(String analyticsId, Client client, DataFrameDataExtractor dataExtractor) {
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.client = Objects.requireNonNull(client);
        this.dataExtractor = Objects.requireNonNull(dataExtractor);
    }

    public void processRowResults(RowResults rowResults) {
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
        if (currentDataFrameRows == null) {
            Optional<List<DataFrameDataExtractor.Row>> nextBatch = getNextBatch();
            if (nextBatch.isPresent() == false) {
                return;
            }
            currentDataFrameRows = nextBatch.get();
            currentResults = new ArrayList<>(currentDataFrameRows.size());
        }
        currentResults.add(rowResults);
        if (currentResults.size() == currentDataFrameRows.size()) {
            joinCurrentResults();
            currentDataFrameRows = null;
        }
    }

    private Optional<List<DataFrameDataExtractor.Row>> getNextBatch() {
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
        for (int i = 0; i < currentDataFrameRows.size(); i++) {
            DataFrameDataExtractor.Row row = currentDataFrameRows.get(i);
            if (row.shouldSkip()) {
                continue;
            }
            RowResults result = currentResults.get(i);
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
}
