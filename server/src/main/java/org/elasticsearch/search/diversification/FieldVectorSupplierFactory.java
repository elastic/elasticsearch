/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

public final class FieldVectorSupplierFactory {
    private FieldVectorSupplierFactory() {}

    public static FieldVectorSupplier getVectorSupplier(String fieldName, DiversifyRetrieverBuilder.RankDocWithSearchHit testHit) {
        if (SemanticTextFieldVectorSupplier.isFieldSemanticTextVector(fieldName, testHit)) {
            return new SemanticTextFieldVectorSupplier(fieldName);
        }

        if (DenseVectorFieldVectorSupplier.canFieldBeDenseVector(fieldName, testHit)) {
            return new DenseVectorFieldVectorSupplier(fieldName);
        }

        return null;
    }
}
