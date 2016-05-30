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
import org.elasticsearch.common.lucene.uid.Versions.DocIdAndVersion;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;

import java.io.IOException;


/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds. */

final class PerThreadIDAndVersionLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    /** terms enum for uid field */
    private final TermsEnum termsEnum;
    /** _version data */
    private final NumericDocValues versions;
    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    /**
     * Initialize lookup for the provided segment
     */
    public PerThreadIDAndVersionLookup(LeafReader reader) throws IOException {
        TermsEnum termsEnum = null;
        NumericDocValues versions = null;

        Fields fields = reader.fields();
        if (fields != null) {
            Terms terms = fields.terms(UidFieldMapper.NAME);
            if (terms != null) {
                termsEnum = terms.iterator();
                assert termsEnum != null;
                versions = reader.getNumericDocValues(VersionFieldMapper.NAME);
                assert versions != null;
            }
        }

        this.versions = versions;
        this.termsEnum = termsEnum;
    }

    /** Return null if id is not found. */
    public DocIdAndVersion lookup(BytesRef id, Bits liveDocs, LeafReaderContext context) throws IOException {
        if (termsEnum.seekExact(id)) {
            // there may be more than one matching docID, in the case of nested docs, so we want the last one:
            docsEnum = termsEnum.postings(docsEnum, 0);
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
                if (liveDocs != null && liveDocs.get(d) == false) {
                    continue;
                }
                docID = d;
            }

            if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                return new DocIdAndVersion(docID, versions.get(docID), context);
            }
        }

        return null;
    }
}
