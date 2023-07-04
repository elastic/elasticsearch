/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

public final class SeqNoPrimaryTermPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.seqNoAndPrimaryTerm() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            NumericDocValues seqNoField = null;
            NumericDocValues primaryTermField = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                seqNoField = readerContext.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
                primaryTermField = readerContext.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                int docId = hitContext.docId();
                long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
                // we have to check the primary term field as it is only assigned for non-nested documents
                if (primaryTermField != null && primaryTermField.advanceExact(docId)) {
                    boolean found = seqNoField.advanceExact(docId);
                    assert found : "found seq no for " + docId + " but not a primary term";
                    seqNo = seqNoField.longValue();
                    primaryTerm = primaryTermField.longValue();
                }
                hitContext.hit().setSeqNo(seqNo);
                hitContext.hit().setPrimaryTerm(primaryTerm);
            }
        };
    }
}
