/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.sillymodel;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.ml.inference.Model;

import java.util.Random;
import java.util.function.BiConsumer;

/**
 * Trivial model whose only purpose is to aid code design
 */
public class SillyModel implements Model {

    private static final String TARGET_FIELD = "hotdog_or_not";

    private final Random random;

    public SillyModel() {
        random = Randomness.get();
    }

    public void infer(IngestDocument document, BiConsumer<IngestDocument, Exception> handler) {
        document.setFieldValue(TARGET_FIELD, random.nextBoolean() ? "hotdog" : "not");
        handler.accept(document, null);
    }
}
