/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;


import org.apache.lucene.util.BytesRef;

import java.util.Objects;

/** A tuple of document id, sequence number, primary term, source and version of a document */
public final class DocIdSeqNoAndSource {
    private final String id;
    private final BytesRef source;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;

    public DocIdSeqNoAndSource(String id, BytesRef source, long seqNo, long primaryTerm, long version) {
        this.id = id;
        this.source = source;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public BytesRef getSource() {
        return source;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocIdSeqNoAndSource that = (DocIdSeqNoAndSource) o;
        return Objects.equals(id, that.id) && Objects.equals(source, that.source)
            && seqNo == that.seqNo && primaryTerm == that.primaryTerm && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, seqNo, primaryTerm, version);
    }

    @Override
    public String toString() {
        return "doc{" + "id='" + id + " seqNo=" + seqNo + " primaryTerm=" + primaryTerm
            + " version=" + version + " source= " + (source != null ? source.utf8ToString() : null) + "}";
    }
}
