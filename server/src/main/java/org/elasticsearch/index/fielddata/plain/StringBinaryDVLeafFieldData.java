/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

final class StringBinaryDVLeafFieldData extends AbstractBinaryDVLeafFieldData {
    StringBinaryDVLeafFieldData(BinaryDocValues values) {
        super(values);
    }

    @Override
    public ScriptDocValues<String> getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }
}
