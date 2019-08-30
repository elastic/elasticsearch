/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.ingest.IngestDocument;

import java.util.function.BiConsumer;

public interface Model {

    /**
     * Perform inference on the input {@code document}.
     *
     * The type of inference is dependent on the implementing model, in general
     * the input is an ingest document and {@code handler} <strong>must</strong>
     * be called either with the updated (or new) document or an {@code Exception}.
     *
     * @param document  Document to infer on
     * @param handler   The handler that must be called
     */
    void infer(IngestDocument document, BiConsumer<IngestDocument, Exception> handler);
}
