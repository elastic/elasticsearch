/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

final class StringBinaryDVLeafFieldData extends AbstractBinaryDVLeafFieldData {

    protected final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

    StringBinaryDVLeafFieldData(BinaryDocValues values, ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory) {
        super(values);

        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getBytesValues(), name);
    }
}
