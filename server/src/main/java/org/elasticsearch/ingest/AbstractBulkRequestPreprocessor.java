/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

public abstract class AbstractBulkRequestPreprocessor implements BulkRequestPreprocessor {

    protected final Supplier<DocumentParsingObserver> documentParsingObserverSupplier;

    protected final IngestMetric ingestMetric = new IngestMetric();

    public AbstractBulkRequestPreprocessor(Supplier<DocumentParsingObserver> documentParsingObserver) {
        this.documentParsingObserverSupplier = documentParsingObserver;
    }

    @Override
    public void processBulkRequest(
        ExecutorService executorService,
        int numberOfActionRequests,
        Iterable<DocWriteRequest<?>> actionRequests,
        final IntConsumer onDropped,
        final BiConsumer<Integer, Exception> onFailure,
        final BiConsumer<Thread, Exception> onCompletion,
        final String executorName
    ) {
        assert numberOfActionRequests > 0 : "numberOfActionRequests must be greater than 0 but was [" + numberOfActionRequests + "]";

        executorService.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                onCompletion.accept(null, e);
            }

            @Override
            protected void doRun() {
                final Thread originalThread = Thread.currentThread();
                try (var refs = new RefCountingRunnable(() -> onCompletion.accept(originalThread, null))) {
                    int slot = 0;
                    for (DocWriteRequest<?> actionRequest : actionRequests) {
                        IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(actionRequest);
                        if (indexRequest != null) {
                            processIndexRequest(indexRequest, slot, refs, onDropped, onFailure);
                        }
                        slot++;
                    }
                }
            }
        });
    }

    protected abstract void processIndexRequest(
        IndexRequest indexRequest,
        int slot,
        RefCountingRunnable refs,
        IntConsumer onDropped,
        BiConsumer<Integer, Exception> onFailure
    );

    /**
     * Updates an index request based on the source of an ingest document, guarding against self-references if necessary.
     */
    protected static void updateIndexRequestSource(final IndexRequest request, final IngestDocument document) {
        boolean ensureNoSelfReferences = document.doNoSelfReferencesCheck();
        // we already check for self references elsewhere (and clear the bit), so this should always be false,
        // keeping the check and assert as a guard against extraordinarily surprising circumstances
        assert ensureNoSelfReferences == false;
        request.source(document.getSource(), request.getContentType(), ensureNoSelfReferences);
    }

    /**
     * Builds a new ingest document from the passed-in index request.
     */
    protected IngestDocument newIngestDocument(final IndexRequest request) {
        return new IngestDocument(
            request.index(),
            request.id(),
            request.version(),
            request.routing(),
            request.versionType(),
            request.sourceAsMap(documentParsingObserverSupplier.get())
        );
    }
}
