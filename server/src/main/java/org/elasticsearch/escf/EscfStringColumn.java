/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/** An ESCF column whose values are all UTF-8 strings (variable-length layout: offset vector + dense byte payload). */
final class EscfStringColumn extends AbstractVarColumn {

    EscfStringColumn(int docCount, FixedBitSet validity, BytesReference data, IntsRef offsets) {
        super(docCount, validity, data, offsets);
    }

    @Override
    byte kind() {
        return EscfColumnKind.STRING;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.STRING;
    }

    @Override
    Text getStringValue(int row) {
        BytesRef ref = getBinaryValue(row);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    AbstractVarColumn newSlice(int count, FixedBitSet sliceValidity, BytesReference sliceData, IntsRef sliceOffsets) {
        return new EscfStringColumn(count, sliceValidity, sliceData, sliceOffsets);
    }
}
