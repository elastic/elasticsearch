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

public class IntsArrayOrderBlockLoaderTests extends AbstractArrayOrderBlockLoaderTests<Integer> {

    @Override
    protected Integer randomValue() {
        return randomInt();
    }

    @Override
    protected BiConsumer<LuceneDocument, Integer> addField() {
        return (d, v) -> d.add(new SortedNumericDocValuesField(FIELD, v));
    }

    @Override
    protected BlockDocValuesReader.DocValuesBlockLoader newLoader(String fieldName) {
        return new IntsBlockLoader(fieldName, true);
    }

    @Override
    protected Object expectedFallbackShape(List<Integer> insertedDistinctValues) {
        return insertedDistinctValues.stream().sorted().toList();
    }
}
