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

    EscfStringColumn(int docCount, FixedBitSet absent, BytesReference data, IntsRef offsets) {
        super(docCount, absent, data, offsets);
    }

    @Override
    byte kind() {
        return EscfColumnKind.STRING;
    }

    @Override
    byte typeByteForPresent(int d) {
        return SourceValueType.STRING;
    }

    @Override
    Text getStringValue(int d) {
        BytesRef ref = getBinaryValue(d);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        // data is kept full/shared; the slice is expressed by adjusting offsets.offset.
        return new EscfStringColumn(
            count,
            windowBitSet(absent, from, count),
            data,
            new IntsRef(offsets.ints, offsets.offset + from, count + 1)
        );
    }

    @Override
    EscfColumnData toColumnData() {
        int byteFrom = offsets.ints[offsets.offset];
        BytesReference newData = data.slice(byteFrom, offsets.ints[offsets.offset + docCount] - byteFrom);
        int[] newOffsets = rebasedOffsets(offsets, docCount);
        return EscfColumnData.ofVarWidth(kind(), docCount, absent, newOffsets, newData);
    }
}
