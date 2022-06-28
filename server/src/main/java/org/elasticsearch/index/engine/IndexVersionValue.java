/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.translog.Translog;

import java.util.Objects;

final class IndexVersionValue extends VersionValue {

    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IndexVersionValue.class);

    private final Translog.Location translogLocation;

    IndexVersionValue(Translog.Location translogLocation, long version, long seqNo, long term) {
        super(version, seqNo, term);
        this.translogLocation = translogLocation;
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(translogLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        IndexVersionValue that = (IndexVersionValue) o;
        return Objects.equals(translogLocation, that.translogLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), translogLocation);
    }

    @Override
    public String toString() {
        return "IndexVersionValue{" + "version=" + version + ", seqNo=" + seqNo + ", term=" + term + ", location=" + translogLocation + '}';
    }

    @Override
    public Translog.Location getLocation() {
        return translogLocation;
    }
}
