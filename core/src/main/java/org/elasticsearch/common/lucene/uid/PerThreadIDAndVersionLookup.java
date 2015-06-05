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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.uid.Versions.DocIdAndVersion;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;


/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds. */

final class PerThreadIDAndVersionLookup {

    private final LeafReaderContext[] readerContexts;
    private final TermsEnum[] termsEnums;
    private final PostingsEnum[] docsEnums;
    // Only used for back compat, to lookup a version from payload:
    private final PostingsEnum[] posEnums;
    private final Bits[] liveDocs;
    private final NumericDocValues[] versions;
    private final int numSegs;
    private final boolean hasDeletions;
    private final boolean[] hasPayloads;

    public PerThreadIDAndVersionLookup(IndexReader r) throws IOException {

        List<LeafReaderContext> leaves = new ArrayList<>(r.leaves());

        readerContexts = leaves.toArray(new LeafReaderContext[leaves.size()]);
        termsEnums = new TermsEnum[leaves.size()];
        docsEnums = new PostingsEnum[leaves.size()];
        posEnums = new PostingsEnum[leaves.size()];
        liveDocs = new Bits[leaves.size()];
        versions = new NumericDocValues[leaves.size()];
        hasPayloads = new boolean[leaves.size()];
        int numSegs = 0;
        boolean hasDeletions = false;
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for(int i=leaves.size()-1;i>=0;i--) {
            LeafReaderContext readerContext = leaves.get(i);
            Fields fields = readerContext.reader().fields();
            if (fields != null) {
                Terms terms = fields.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    readerContexts[numSegs] = readerContext;
                    hasPayloads[numSegs] = terms.hasPayloads();
                    termsEnums[numSegs] = terms.iterator();
                    assert termsEnums[numSegs] != null;
                    liveDocs[numSegs] = readerContext.reader().getLiveDocs();
                    hasDeletions |= readerContext.reader().hasDeletions();
                    versions[numSegs] = readerContext.reader().getNumericDocValues(VersionFieldMapper.NAME);
                    numSegs++;
                }
            }
        }
        this.numSegs = numSegs;
        this.hasDeletions = hasDeletions;
    }

    /** Return null if id is not found. */
    public DocIdAndVersion lookup(BytesRef id) throws IOException {
        for(int seg=0;seg<numSegs;seg++) {
            if (termsEnums[seg].seekExact(id)) {

                NumericDocValues segVersions = versions[seg];
                if (segVersions != null || hasPayloads[seg] == false) {
                    // Use NDV to retrieve the version, in which case we only need PostingsEnum:

                    // there may be more than one matching docID, in the case of nested docs, so we want the last one:
                    PostingsEnum docs = docsEnums[seg] = termsEnums[seg].postings(liveDocs[seg], docsEnums[seg], 0);
                    int docID = DocIdSetIterator.NO_MORE_DOCS;
                    for (int d = docs.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docs.nextDoc()) {
                        docID = d;
                    }

                    if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                        if (segVersions != null) {
                            return new DocIdAndVersion(docID, segVersions.get(docID), readerContexts[seg]);
                        } else {
                            // _uid found, but no doc values and no payloads
                            return new DocIdAndVersion(docID, Versions.NOT_SET, readerContexts[seg]);
                        }
                    } else {
                        assert hasDeletions;
                        continue;
                    }
                }

                // ... but used to be stored as payloads; in this case we must use PostingsEnum
                PostingsEnum dpe = posEnums[seg] = termsEnums[seg].postings(liveDocs[seg], posEnums[seg], PostingsEnum.PAYLOADS);
                assert dpe != null; // terms has payloads
                for (int d = dpe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = dpe.nextDoc()) {
                    dpe.nextPosition();
                    final BytesRef payload = dpe.getPayload();
                    if (payload != null && payload.length == 8) {
                        // TODO: does this break the nested docs case?  we are not returning the last matching docID here?
                        return new DocIdAndVersion(d, Numbers.bytesToLong(payload), readerContexts[seg]);
                    }
                }
            }
        }

        return null;
    }

    // TODO: add reopen method to carry over re-used enums...?
}
