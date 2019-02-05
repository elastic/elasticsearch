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


import java.util.Objects;

/** A tuple of document id, sequence number and primary term of a document */
public final class DocIdSeqNoAndTerm {
    private final String id;
    private final long seqNo;
    private final long primaryTerm;

    public DocIdSeqNoAndTerm(String id, long seqNo, long primaryTerm) {
        this.id = id;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    public String getId() {
        return id;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocIdSeqNoAndTerm that = (DocIdSeqNoAndTerm) o;
        return Objects.equals(id, that.id) && seqNo == that.seqNo && primaryTerm == that.primaryTerm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, seqNo, primaryTerm);
    }

    @Override
    public String toString() {
        return "DocIdSeqNoAndTerm{" + "id='" + id + " seqNo=" + seqNo + " primaryTerm=" + primaryTerm + "}";
    }
}
