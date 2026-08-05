/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.elasticsearch.index.mapper.LuceneDocument;

import java.util.List;
import java.util.function.BiConsumer;

public class BooleansArrayOrderBlockLoaderTests extends AbstractArrayOrderBlockLoaderTests<Boolean> {

    @Override
    protected Boolean randomValue() {
        return randomBoolean();
    }

    @Override
    protected BiConsumer<LuceneDocument, Boolean> addField() {
        return (d, v) -> d.add(new SortedNumericDocValuesField(FIELD, v ? 1L : 0L));
    }

    @Override
    protected BlockDocValuesReader.DocValuesBlockLoader newLoader(String fieldName) {
        return new BooleansBlockLoader(fieldName, true);
    }

    @Override
    protected Object expectedFallbackShape(List<Boolean> insertedDistinctValues) {
        return insertedDistinctValues.stream().sorted().toList();
    }
}
