/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        if (!super.equals(o)) return false;

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
