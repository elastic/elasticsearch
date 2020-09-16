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
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;

public final class SeqNoPrimaryTermPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(SearchContext context, SearchLookup lookup) throws IOException {
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
            public void process(HitContext hitContext) throws IOException {
                int docId = hitContext.docId();
                long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
                // we have to check the primary term field as it is only assigned for non-nested documents
                if (primaryTermField != null && primaryTermField.advanceExact(docId)) {
                    boolean found = seqNoField.advanceExact(docId);
                    assert found: "found seq no for " + docId + " but not a primary term";
                    seqNo = seqNoField.longValue();
                    primaryTerm = primaryTermField.longValue();
                }
                hitContext.hit().setSeqNo(seqNo);
                hitContext.hit().setPrimaryTerm(primaryTerm);
            }
        };
    }
}
