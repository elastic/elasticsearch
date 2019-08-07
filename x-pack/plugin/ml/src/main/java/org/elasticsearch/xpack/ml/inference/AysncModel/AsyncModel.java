/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.AysncModel;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.ml.inference.Model;

import java.util.function.BiConsumer;

public class AsyncModel implements Model {

    @Override
    public void infer(IngestDocument document, BiConsumer<IngestDocument, Exception> handler) {

    }
}
