/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

public final class MultiValuedBinaryDVLeafFieldData implements LeafFieldData {
    private final SortedBinaryDocValues values;
    private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

    MultiValuedBinaryDVLeafFieldData(SortedBinaryDocValues values, ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory) {
        super();
        this.values = values;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return values;
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getBytesValues(), name);
    }

}
