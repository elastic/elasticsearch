/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.io.IOException;
import java.util.Objects;

final class CombinedDocValues {
    private final NumericDocValues versionDV;
    private final NumericDocValues seqNoDV;
    private final NumericDocValues primaryTermDV;
    private final NumericDocValues tombstoneDV;
    private final NumericDocValues recoverySource;

    CombinedDocValues(LeafReader leafReader) throws IOException {
        this.versionDV = Objects.requireNonNull(leafReader.getNumericDocValues(VersionFieldMapper.NAME), "VersionDV is missing");
        this.seqNoDV = Objects.requireNonNull(leafReader.getNumericDocValues(SeqNoFieldMapper.NAME), "SeqNoDV is missing");
        this.primaryTermDV = Objects.requireNonNull(
            leafReader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME), "PrimaryTermDV is missing");
        this.tombstoneDV = leafReader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
        this.recoverySource = leafReader.getNumericDocValues(SourceFieldMapper.RECOVERY_SOURCE_NAME);
    }

    long docVersion(int segmentDocId) throws IOException {
        assert versionDV.docID() < segmentDocId;
        if (versionDV.advanceExact(segmentDocId) == false) {
            assert false : "DocValues for field [" + VersionFieldMapper.NAME + "] is not found";
            throw new IllegalStateException("DocValues for field [" + VersionFieldMapper.NAME + "] is not found");
        }
        return versionDV.longValue();
    }

    long docSeqNo(int segmentDocId) throws IOException {
        assert seqNoDV.docID() < segmentDocId;
        if (seqNoDV.advanceExact(segmentDocId) == false) {
            assert false : "DocValues for field [" + SeqNoFieldMapper.NAME + "] is not found";
            throw new IllegalStateException("DocValues for field [" + SeqNoFieldMapper.NAME + "] is not found");
        }
        return seqNoDV.longValue();
    }

    long docPrimaryTerm(int segmentDocId) throws IOException {
        // We exclude non-root nested documents when querying changes, every returned document must have primary term.
        assert primaryTermDV.docID() < segmentDocId;
        if (primaryTermDV.advanceExact(segmentDocId) == false) {
            assert false : "DocValues for field [" + SeqNoFieldMapper.PRIMARY_TERM_NAME + "] is not found";
            throw new IllegalStateException("DocValues for field [" + SeqNoFieldMapper.PRIMARY_TERM_NAME + "] is not found");
        }
        return primaryTermDV.longValue();
    }

    boolean isTombstone(int segmentDocId) throws IOException {
        if (tombstoneDV == null) {
            return false;
        }
        assert tombstoneDV.docID() < segmentDocId;
        return tombstoneDV.advanceExact(segmentDocId) && tombstoneDV.longValue() > 0;
    }

    boolean hasRecoverySource(int segmentDocId) throws IOException {
        if (recoverySource == null) {
            return false;
        }
        assert recoverySource.docID() < segmentDocId;
        return recoverySource.advanceExact(segmentDocId);
    }
}
