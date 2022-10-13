/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public class UnsignedLongIndexFieldData extends IndexNumericFieldData {
    private final IndexNumericFieldData signedLongIFD;
    protected final ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory;

    UnsignedLongIndexFieldData(
        IndexNumericFieldData signedLongFieldData,
        ToScriptFieldFactory<SortedNumericDocValues> toScriptFieldFactory
    ) {
        this.signedLongIFD = signedLongFieldData;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public String getFieldName() {
        return signedLongIFD.getFieldName();
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return signedLongIFD.getValuesSourceType();
    }

    @Override
    public LeafNumericFieldData load(LeafReaderContext context) {
        return new UnsignedLongLeafFieldData(signedLongIFD.load(context), toScriptFieldFactory);
    }

    @Override
    public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new UnsignedLongLeafFieldData(signedLongIFD.loadDirect(context), toScriptFieldFactory);
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return false;
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.LONG;
    }

}
