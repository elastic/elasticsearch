/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

public class DoublesBlockLoader extends AbstractDoublesFromDocValuesBlockLoader {
    public DoublesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        super(fieldName, toDouble);
    }

    @Override
    protected AllReader singletonReader(NumericDocValues docValues, BlockDocValuesReader.ToDouble toDouble) {
        return new Singleton(docValues, toDouble);
    }

    @Override
    protected AllReader sortedReader(SortedNumericDocValues docValues, BlockDocValuesReader.ToDouble toDouble) {
        return new Sorted(docValues, toDouble);
    }

    @Override
    public String toString() {
        return "DoublesFromDocValues[" + fieldName + "]";
    }
}
