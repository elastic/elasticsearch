/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.RamUsageEstimator;

/** Holds a deleted version, which just adds a timestamp to {@link VersionValue} so we know when we can expire the deletion. */

final class DeleteVersionValue extends VersionValue {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DeleteVersionValue.class);

    final long time;

    DeleteVersionValue(long version,long seqNo, long term, long time) {
        super(version, seqNo, term);
        this.time = time;
    }

    @Override
    public boolean isDelete() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;

        DeleteVersionValue that = (DeleteVersionValue) o;

        return time == that.time;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (time ^ (time >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DeleteVersionValue{" +
            "version=" + version +
            ", seqNo=" + seqNo +
            ", term=" + term +
            ",time=" + time +
            '}';
    }
}
