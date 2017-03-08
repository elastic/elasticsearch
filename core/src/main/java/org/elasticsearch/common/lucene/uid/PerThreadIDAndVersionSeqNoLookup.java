package org.elasticsearch.common.lucene.uid;

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

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbersService;

import java.io.IOException;


/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds. */

final class PerThreadIDAndVersionSeqNoLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    /** terms enum for uid field */
    private final TermsEnum termsEnum;
    /** _version data */
    private final NumericDocValues versions;
    /** _seq_no data */
    private final NumericDocValues seqNos;
    /** _primary_term data */
    private final NumericDocValues primaryTerms;
    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    private final Object readerKey;

    /**
     * Initialize lookup for the provided segment
     */
    PerThreadIDAndVersionSeqNoLookup(LeafReader reader) throws IOException {
        Fields fields = reader.fields();
        Terms terms = fields.terms(UidFieldMapper.NAME);
        termsEnum = terms.iterator();
        assert termsEnum != null;
        versions = reader.getNumericDocValues(VersionFieldMapper.NAME);
        assert versions != null;
        seqNos = reader.getNumericDocValues(SeqNoFieldMapper.NAME);
        primaryTerms = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        readerKey = reader.getCoreCacheKey();
    }

    /** Return null if id is not found. */
    public DocIdAndVersion lookupVersion(BytesRef id, Bits liveDocs, LeafReaderContext context) throws IOException {
        assert context.reader().getCoreCacheKey().equals(readerKey);
        int docID = getDocID(id, liveDocs);

        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            return new DocIdAndVersion(docID, versions.get(docID), context);
        } else {
            return null;
        }
    }

    /** returns the internal lucene doc id for the given id bytes. {@link DocIdSetIterator#NO_MORE_DOCS} is returned if not found */
    private int getDocID(BytesRef id, Bits liveDocs) throws IOException {
        if (termsEnum.seekExact(id)) {
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            // there may be more than one matching docID, in the case of nested docs, so we want the last one:
            docsEnum = termsEnum.postings(docsEnum, 0);
            for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
                if (liveDocs != null && liveDocs.get(d) == false) {
                    continue;
                }
                docID = d;
            }
            return docID;
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    /** Return null if id is not found. */
    public DocIdAndSeqNo lookupSequenceNo(BytesRef id, Bits liveDocs, LeafReaderContext context) throws IOException {
        assert context.reader().getCoreCacheKey().equals(readerKey);
        int docID = getDocID(id, liveDocs);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            return new DocIdAndSeqNo(docID, seqNos == null ? SequenceNumbersService.UNASSIGNED_SEQ_NO : seqNos.get(docID), context);
        } else {
            return null;
        }
    }

    /** returns 0 if the primary term is not found */
    public long lookUpPrimaryTerm(int docID) throws IOException {
            return primaryTerms == null ? 0  : primaryTerms.get(docID);
    }
}
