/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.search.SearchHit;

import java.util.Objects;

/**
 * Simple class to keep track of information needed for optimistic concurrency
 */
public class SeqNoPrimaryTermAndIndex {
    private final long seqNo;
    private final long primaryTerm;
    private final String index;

    public static SeqNoPrimaryTermAndIndex fromSearchHit(SearchHit hit) {
        return new SeqNoPrimaryTermAndIndex(hit.getSeqNo(), hit.getPrimaryTerm(), hit.getIndex());
    }

    public static SeqNoPrimaryTermAndIndex fromIndexResponse(IndexResponse response) {
        return new SeqNoPrimaryTermAndIndex(response.getSeqNo(), response.getPrimaryTerm(), response.getIndex());
    }

    SeqNoPrimaryTermAndIndex(long seqNo, long primaryTerm, String index) {
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.index = index;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqNo, primaryTerm, index);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SeqNoPrimaryTermAndIndex other = (SeqNoPrimaryTermAndIndex) obj;
        return Objects.equals(seqNo, other.seqNo)
            && Objects.equals(primaryTerm, other.primaryTerm)
            && Objects.equals(index, other.index);
    }

    @Override
    public String toString() {
        return "{seqNo=" + seqNo + ",primaryTerm=" + primaryTerm + ",index=" + index + "}";
    }
}
