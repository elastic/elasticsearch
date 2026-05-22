/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.util.List;
import java.util.function.BiConsumer;

public class BytesRefsFromBinaryMultiSeparateCountArrayOrderBlockLoaderTests extends AbstractArrayOrderBlockLoaderTests<BytesRef> {

    @Override
    protected BytesRef randomValue() {
        return new BytesRef(randomAlphanumericOfLength(between(1, 8)));
    }

    @Override
    protected BiConsumer<LuceneDocument, BytesRef> addField() {
        return (d, v) -> MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(d, FIELD, v);
    }

    @Override
    protected BlockDocValuesReader.DocValuesBlockLoader newLoader(String fieldName) {
        return new BytesRefsFromBinaryMultiSeparateCountBlockLoader(fieldName, true);
    }

    @Override
    protected Object expectedFallbackShape(List<BytesRef> insertedDistinctValues) {
        // MultiValuedBinaryDocValuesField defaults to SORTED_UNIQUE, so the blob plays back sorted (dedup is a no-op on distinct input)
        return insertedDistinctValues.stream().sorted().toList();
    }
}
