/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpBody;

import java.util.ArrayList;
import java.util.function.Consumer;

public class RestContentAggregator {

    private static void replaceBody(RestRequest restRequest, ReleasableBytesReference aggregate) {
        restRequest.getHttpRequest().setBody(new HttpBody.ByteRefHttpBody(aggregate));
    }

    /**
     * Aggregates content of the RestRequest and notifies consumer with updated, in-place, RestRequest.
     * If content is already aggregated then does nothing.
     */
    public static void aggregate(RestRequest restRequest, Consumer<RestRequest> resultConsumer) {
        final var httpRequest = restRequest.getHttpRequest();
        switch (httpRequest.body()) {
            case HttpBody.Full full -> resultConsumer.accept(restRequest);
            case HttpBody.Stream stream -> {
                if (httpRequest.contentLengthHeader() == 0) {
                    stream.close();
                    replaceBody(restRequest, ReleasableBytesReference.empty());
                    resultConsumer.accept(restRequest);
                } else {
                    final var aggregationHandler = new AggregationChunkHandler(restRequest, resultConsumer);
                    stream.setHandler(aggregationHandler);
                    stream.next();
                }
            }
        }
    }

    private static class AggregationChunkHandler implements HttpBody.ChunkHandler {
        final RestRequest restRequest;
        final Consumer<RestRequest> resultConsumer;
        final HttpBody.Stream stream;
        boolean closing;
        ArrayList<ReleasableBytesReference> chunks;

        private AggregationChunkHandler(RestRequest restRequest, Consumer<RestRequest> resultConsumer) {
            this.restRequest = restRequest;
            this.resultConsumer = resultConsumer;
            this.stream = restRequest.getHttpRequest().body().asStream();
        }

        @Override
        public void onNext(ReleasableBytesReference chunk, boolean isLast) {
            if (closing) {
                chunk.close();
                return;
            }
            if (isLast == false) {
                if (chunks == null) {
                    chunks = new ArrayList<>(); // allocate array only when there is more than one chunk
                }
                chunks.add(chunk);
                stream.next();
            } else {
                if (chunks == null) {
                    replaceBody(restRequest, chunk);
                    resultConsumer.accept(restRequest);
                } else {
                    chunks.add(chunk);
                    var comp = CompositeBytesReference.of(chunks.toArray(new ReleasableBytesReference[0]));
                    var relComp = new ReleasableBytesReference(comp, Releasables.wrap(chunks));
                    replaceBody(restRequest, relComp);
                    resultConsumer.accept(restRequest);
                }
            }
        }

        @Override
        public void close() {
            if (closing == false) {
                closing = true;
                if (chunks != null) {
                    Releasables.close(chunks);
                    chunks = null;
                }
            }
        }
    }

}
