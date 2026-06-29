/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfKeyValueReader;
import org.elasticsearch.xcontent.Text;

/**
 * A view of a single column across all rows in a {@link SourceBatch}, providing typed
 * random-access to values by document index.
 *
 * <p>On the current row-major (EIRF) implementation each getter performs a row scan to
 * locate the column value. A future column-major implementation will make access direct.
 *
 * <p>All getters are pure reads and do not advance any cursor state.
 */
public interface SourceColumn {

    /** The leaf column index this view covers. */
    int columnIndex();

    /** The number of documents in the backing batch. */
    int docCount();

    /** The raw type byte for {@code docIndex}; returns {@link org.elasticsearch.eirf.EirfType#ABSENT} when absent. */
    byte getTypeByte(int docIndex);

    /** Returns {@code true} if the value is absent for {@code docIndex}. */
    boolean isAbsent(int docIndex);

    /** Returns {@code true} if the value is an explicit {@code null} for {@code docIndex}. */
    boolean isNull(int docIndex);

    boolean getBooleanValue(int docIndex);

    int getIntValue(int docIndex);

    float getFloatValue(int docIndex);

    long getLongValue(int docIndex);

    double getDoubleValue(int docIndex);

    Text getStringValue(int docIndex);

    BytesRef getBinaryValue(int docIndex);

    EirfKeyValueReader getKeyValue(int docIndex);

    EirfArrayReader getArrayValue(int docIndex);

    /**
     * Returns a forward {@link SourceColumnCursor} over this column. The default delegates to the
     * random-access getters; implementations with a directly iterable representation override this with
     * a specialized cursor that decodes each document once without polymorphic per-value dispatch.
     */
    default SourceColumnCursor cursor() {
        return new RandomAccessSourceColumnCursor(this);
    }
}
