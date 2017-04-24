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

final class PerThreadIDVersionAndSeqNoLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    /** terms enum for uid field */
    private final TermsEnum termsEnum;

    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    /** used for assertions to make sure class usage meets assumptions */
    private final Object readerKey;

    /**
     * Initialize lookup for the provided segment
     */
    PerThreadIDVersionAndSeqNoLookup(LeafReader reader) throws IOException {
        Fields fields = reader.fields();
        Terms terms = fields.terms(UidFieldMapper.NAME);
        termsEnum = terms.iterator();
        if (termsEnum == null) {
            throw new IllegalArgumentException("reader misses the [" + UidFieldMapper.NAME + "] field");
        }
        if (reader.getNumericDocValues(VersionFieldMapper.NAME) == null) {
            throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field");
        }

        Object readerKey = null;
        assert (readerKey = reader.getCoreCacheHelper().getKey()) != null;
        this.readerKey = readerKey;
    }

    /** Return null if id is not found. */
    public DocIdAndVersion lookupVersion(BytesRef id, Bits liveDocs, LeafReaderContext context)
        throws IOException {
        assert context.reader().getCoreCacheHelper().getKey().equals(readerKey) :
            "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, liveDocs);

        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final NumericDocValues versions = context.reader().getNumericDocValues(VersionFieldMapper.NAME);
            if (versions == null) {
                throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field");
            }
            if (versions.advanceExact(docID) == false) {
                throw new IllegalArgumentException("Document [" + docID + "] misses the [" + VersionFieldMapper.NAME + "] field");
            }
            return new DocIdAndVersion(docID, versions.longValue(), context);
        } else {
            return null;
        }
    }

    /**
     * returns the internal lucene doc id for the given id bytes.
     * {@link DocIdSetIterator#NO_MORE_DOCS} is returned if not found
     * */
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
    DocIdAndSeqNo lookupSeqNo(BytesRef id, Bits liveDocs, LeafReaderContext context) throws IOException {
        assert context.reader().getCoreCacheHelper().getKey().equals(readerKey) :
            "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, liveDocs);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            NumericDocValues seqNos = context.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
            long seqNo;
            if (seqNos != null && seqNos.advanceExact(docID)) {
                seqNo = seqNos.longValue();
            } else {
                seqNo =  SequenceNumbersService.UNASSIGNED_SEQ_NO;
            }
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }

    /**
     * returns 0 if the primary term is not found.
     *
     * Note that 0 is an illegal primary term. See {@link org.elasticsearch.cluster.metadata.IndexMetaData#primaryTerm(int)}
     **/
    long lookUpPrimaryTerm(int docID, LeafReader reader) throws IOException {
        NumericDocValues primaryTerms = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        if (primaryTerms != null && primaryTerms.advanceExact(docID)) {
            return primaryTerms.longValue();
        } else {
            return 0;
        }
    }
}
