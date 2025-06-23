/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.process.StateToProcessWriterHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * A {@code StateStreamer} fetches the various state documents and
 * writes them into a stream. It allows cancellation via its
 *{@link #cancel()} method; cancellation is checked between writing
 * the various state documents.
 */
public class StateStreamer {

    private static final Logger LOGGER = LogManager.getLogger(StateStreamer.class);

    private final Client client;
    private volatile boolean isCancelled;

    public StateStreamer(Client client) {
        this.client = Objects.requireNonNull(client);
    }

    /**
     * Cancels the state streaming at the first opportunity.
     */
    public void cancel() {
        isCancelled = true;
    }

    /**
     * Given a model snapshot, get the corresponding state and write it to the supplied
     * stream.  If there are multiple state documents they are separated using <code>'\0'</code>
     * when written to the stream.
     *
     * Because we have a rule that we will not open a legacy job in the current product version
     * we don't have to worry about legacy document IDs here.
     *
     * @param jobId         the job id
     * @param modelSnapshot the model snapshot to be restored
     * @param restoreStream the stream to write the state to
     */
    public void restoreStateToStream(String jobId, ModelSnapshot modelSnapshot, OutputStream restoreStream) throws IOException {
        String indexName = AnomalyDetectorsIndex.jobStateIndexPattern();

        // First try to restore model state.
        for (String stateDocId : modelSnapshot.stateDocumentIds()) {
            if (isCancelled) {
                return;
            }

            LOGGER.trace("ES API CALL: get ID {} from index {}", stateDocId, indexName);

            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
                SearchResponse stateResponse = client.prepareSearch(indexName)
                    .setSize(1)
                    .setQuery(QueryBuilders.idsQuery().addIds(stateDocId))
                    .get();
                try {
                    if (stateResponse.getHits().getHits().length == 0) {
                        LOGGER.error(
                            "Expected {} documents for model state for {} snapshot {} but failed to find {}",
                            modelSnapshot.getSnapshotDocCount(),
                            jobId,
                            modelSnapshot.getSnapshotId(),
                            stateDocId
                        );
                        break;
                    }
                    writeStateToStream(stateResponse.getHits().getAt(0).getSourceRef(), restoreStream);
                } finally {
                    stateResponse.decRef();
                }
            }
        }

        // Secondly try to restore categorizer state. This must come after model state because that's
        // the order the C++ process expects. There are no snapshots for this, so the IDs simply
        // count up until a document is not found. It's NOT an error to have no categorizer state.
        int docNum = 0;
        while (true) {
            if (isCancelled) {
                return;
            }

            String docId = CategorizerState.documentId(jobId, ++docNum);

            LOGGER.trace("ES API CALL: get ID {} from index {}", docId, indexName);

            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
                SearchResponse stateResponse = client.prepareSearch(indexName)
                    .setSize(1)
                    .setQuery(QueryBuilders.idsQuery().addIds(docId))
                    .get();
                try {
                    if (stateResponse.getHits().getHits().length == 0) {
                        break;
                    }
                    writeStateToStream(stateResponse.getHits().getAt(0).getSourceRef(), restoreStream);
                } finally {
                    stateResponse.decRef();
                }
            }
        }

    }

    private void writeStateToStream(BytesReference source, OutputStream stream) throws IOException {
        if (isCancelled) {
            return;
        }

        StateToProcessWriterHelper.writeStateToStream(source, stream);
    }
}
