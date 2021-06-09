/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.translog.Translog;

import java.util.Collection;
import java.util.Collections;

abstract class VersionValue implements Accountable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(VersionValue.class);

    /** the version of the document. used for versioned indexed operations and as a BWC layer, where no seq# are set yet */
    final long version;

    /** the seq number of the operation that last changed the associated uuid */
    final long seqNo;
    /** the term of the operation that last changed the associated uuid */
    final long term;

    VersionValue(long version, long seqNo, long term) {
        this.version = version;
        this.seqNo = seqNo;
        this.term = term;
    }

    public boolean isDelete() {
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VersionValue that = (VersionValue) o;

        if (version != that.version) return false;
        if (seqNo != that.seqNo) return false;
        return term == that.term;
    }

    @Override
    public int hashCode() {
        int result = (int) (version ^ (version >>> 32));
        result = 31 * result + (int) (seqNo ^ (seqNo >>> 32));
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "VersionValue{" +
            "version=" + version +
            ", seqNo=" + seqNo +
            ", term=" + term +
            '}';
    }

    /**
     * Returns the translog location for this version value or null. This is optional and might not be tracked all the time.
     */
    @Nullable
    public Translog.Location getLocation() {
        return null;
    }
}
