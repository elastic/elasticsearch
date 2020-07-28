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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.utils.persistence.LimitAwareBulkIndexer;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
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
    private final Settings settings;
    private final TaskId parentTaskId;
    private final DataFrameDataExtractor dataExtractor;
    private final ResultsPersisterService resultsPersisterService;
    private final Iterator<DataFrameDataExtractor.Row> dataFrameRowsIterator;
    private LinkedList<RowResults> currentResults;
    private volatile String failure;
    private volatile boolean isCancelled;

    DataFrameRowsJoiner(String analyticsId, Settings settings, TaskId parentTaskId, DataFrameDataExtractor dataExtractor,
                        ResultsPersisterService resultsPersisterService) {
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.settings = Objects.requireNonNull(settings);
        this.parentTaskId = Objects.requireNonNull(parentTaskId);
        this.dataExtractor = Objects.requireNonNull(dataExtractor);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.dataFrameRowsIterator = new ResultMatchingDataFrameRows();
        this.currentResults = new LinkedList<>();
    }

    @Nullable
    String getFailure() {
        return failure;
    }

    void processRowResults(RowResults rowResults) {
        if (failure != null) {
            // If we are in failed state we drop the results but we let the processor
            // parse the output
            return;
        }

        try {
            addResultAndJoinIfEndOfBatch(rowResults);
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Failed to join results ", analyticsId), e);
            failure = "[" + analyticsId + "] Failed to join results: " + e.getMessage();
        }
    }

    void cancel() {
        isCancelled = true;
    }

    private void addResultAndJoinIfEndOfBatch(RowResults rowResults) {
        currentResults.add(rowResults);
        if (currentResults.size() == RESULTS_BATCH_SIZE) {
            joinCurrentResults();
        }
    }

    private void joinCurrentResults() {
        try (LimitAwareBulkIndexer bulkIndexer = new LimitAwareBulkIndexer(settings, this::executeBulkRequest)) {
            while (currentResults.isEmpty() == false) {
                RowResults result = currentResults.pop();
                DataFrameDataExtractor.Row row = dataFrameRowsIterator.next();
                checkChecksumsMatch(row, result);
                bulkIndexer.addAndExecuteIfNeeded(createIndexRequest(result, row.getHit()));
            }
        }

        currentResults = new LinkedList<>();
    }

    private void executeBulkRequest(BulkRequest bulkRequest) {
        bulkRequest.setParentTask(parentTaskId);
        resultsPersisterService.bulkIndexWithHeadersWithRetry(
            dataExtractor.getHeaders(),
            bulkRequest,
            analyticsId,
            () -> isCancelled == false,
            errorMsg -> {});
    }

    private void checkChecksumsMatch(DataFrameDataExtractor.Row row, RowResults result) {
        if (row.getChecksum() != result.getChecksum()) {
            String msg = "Detected checksum mismatch for document with id [" + row.getHit().getId() + "]; ";
            msg += "expected [" + row.getChecksum() + "] but result had [" + result.getChecksum() + "]; ";
            msg += "this implies the data frame index [" + row.getHit().getIndex() + "] was modified while the analysis was running. ";
            msg += "We rely on this index being immutable during a running analysis and so the results will be unreliable.";
            throw ExceptionsHelper.serverError(msg);
        }
    }

    private IndexRequest createIndexRequest(RowResults result, SearchHit hit) {
        Map<String, Object> source = new LinkedHashMap<>(hit.getSourceAsMap());
        source.putAll(result.getResults());
        IndexRequest indexRequest = new IndexRequest(hit.getIndex());
        indexRequest.id(hit.getId());
        indexRequest.source(source);
        indexRequest.opType(DocWriteRequest.OpType.INDEX);
        indexRequest.setParentTask(parentTaskId);
        return indexRequest;
    }

    @Override
    public void close() {
        try {
            joinCurrentResults();
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Failed to join results", analyticsId), e);
            failure = "[" + analyticsId + "] Failed to join results: " + e.getMessage();
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

    private class ResultMatchingDataFrameRows implements Iterator<DataFrameDataExtractor.Row> {

        private List<DataFrameDataExtractor.Row> currentDataFrameRows = Collections.emptyList();
        private int currentDataFrameRowsIndex;

        @Override
        public boolean hasNext() {
            return dataExtractor.hasNext() || currentDataFrameRowsIndex < currentDataFrameRows.size();
        }

        @Override
        public DataFrameDataExtractor.Row next() {
            DataFrameDataExtractor.Row row = null;
            while (hasNoMatch(row) && hasNext()) {
                advanceToNextBatchIfNecessary();
                row = currentDataFrameRows.get(currentDataFrameRowsIndex++);
            }

            if (hasNoMatch(row)) {
                throw ExceptionsHelper.serverError("no more data frame rows could be found while joining results");
            }
            return row;
        }

        private boolean hasNoMatch(DataFrameDataExtractor.Row row) {
            return row == null || row.shouldSkip() || row.isTraining() == false;
        }

        private void advanceToNextBatchIfNecessary() {
            if (currentDataFrameRowsIndex >= currentDataFrameRows.size()) {
                currentDataFrameRows = getNextDataRowsBatch().orElse(Collections.emptyList());
                currentDataFrameRowsIndex = 0;
            }
        }

        private Optional<List<DataFrameDataExtractor.Row>> getNextDataRowsBatch() {
            try {
                return dataExtractor.next();
            } catch (IOException e) {
                throw ExceptionsHelper.serverError("error reading next batch of data frame rows [" + e.getMessage() + "]");
            }
        }
    }
}
