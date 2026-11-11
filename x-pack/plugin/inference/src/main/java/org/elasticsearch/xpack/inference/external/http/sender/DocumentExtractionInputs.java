/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.inference.DocumentExtractionRequest;
import org.elasticsearch.inference.InferenceString;

import java.util.List;
import java.util.Objects;

public class DocumentExtractionInputs extends InferenceInputs {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DocumentExtractionInputs.class);

    public static DocumentExtractionInputs fromDocumentExtractionRequest(DocumentExtractionRequest request) {
        return new DocumentExtractionInputs(request.inputs());
    }

    private final List<InferenceString> documents;

    public DocumentExtractionInputs(List<InferenceString> documents) {
        super(false);
        this.documents = Objects.requireNonNull(documents);
    }

    public List<InferenceString> getDocuments() {
        return documents;
    }

    @Override
    public boolean isSingleInput() {
        return documents.size() == 1;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOfCollection(documents);
    }
}
