/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;

final class BytesBinaryDVLeafFieldData extends AbstractBinaryDVLeafFieldData {
    BytesBinaryDVLeafFieldData(BinaryDocValues values) {
        super(values);
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return new BinaryDocValuesField(getBytesValues(), name);
    }
}
