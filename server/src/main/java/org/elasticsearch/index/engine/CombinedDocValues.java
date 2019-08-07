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
            throw new IllegalStateException("DocValues for field [" + VersionFieldMapper.NAME + "] is not found");
        }
        return versionDV.longValue();
    }

    long docSeqNo(int segmentDocId) throws IOException {
        assert seqNoDV.docID() < segmentDocId;
        if (seqNoDV.advanceExact(segmentDocId) == false) {
            throw new IllegalStateException("DocValues for field [" + SeqNoFieldMapper.NAME + "] is not found");
        }
        return seqNoDV.longValue();
    }

    long docPrimaryTerm(int segmentDocId) throws IOException {
        if (primaryTermDV == null) {
            return -1L;
        }
        assert primaryTermDV.docID() < segmentDocId;
        // Use -1 for docs which don't have primary term. The caller considers those docs as nested docs.
        if (primaryTermDV.advanceExact(segmentDocId) == false) {
            return -1;
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
