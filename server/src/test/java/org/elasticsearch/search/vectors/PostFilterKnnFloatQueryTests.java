/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;

public class PostFilterKnnFloatQueryTests extends AbstractPostFilterKnnQueryTests {

    @Override
    protected AssertingKnnQuery.VectorType vectorType() {
        return AssertingKnnQuery.VectorType.FLOAT;
    }

    @Override
    protected void addVectorField(Document doc, String field, float value) {
        doc.add(new KnnFloatVectorField(field, new float[] { value }));
    }
}
