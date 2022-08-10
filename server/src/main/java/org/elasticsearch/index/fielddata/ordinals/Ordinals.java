/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;

/**
 * A thread safe ordinals abstraction. Ordinals can only be positive integers.
 */
public abstract class Ordinals implements Accountable {

    public static final ValuesHolder NO_VALUES = ord -> { throw new UnsupportedOperationException(); };

    /**
     * The memory size this ordinals take.
     */
    @Override
    public abstract long ramBytesUsed();

    public abstract SortedSetDocValues ordinals(ValuesHolder values);

    public final SortedSetDocValues ordinals() {
        return ordinals(NO_VALUES);
    }

    public interface ValuesHolder {

        BytesRef lookupOrd(long ord);
    }

}
