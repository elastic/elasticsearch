/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.io.Reader;

/**
 *
 */
// TODO: LUCENE 4 UPGRADE: Store version as doc values instead of as a payload.
public class UidField extends Field {

    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final AtomicReaderContext reader;

        public DocIdAndVersion(int docId, long version, AtomicReaderContext reader) {
            this.docId = docId;
            this.version = version;
            this.reader = reader;
        }
    }

    // this works fine for nested docs since they don't have the payload which has the version
    // so we iterate till we find the one with the payload
    public static DocIdAndVersion loadDocIdAndVersion(AtomicReaderContext context, Term term) {
        int docId = Lucene.NO_DOC;
        try {
            Terms terms = context.reader().terms(term.field());
            if (terms == null) {
                return null;
            }
            final TermsEnum termsEnum = terms.iterator(null);
            if (termsEnum == null) {
                return null;
            }
            if (!termsEnum.seekExact(term.bytes())) {
                return null;
            }
            DocsAndPositionsEnum uid = termsEnum.docsAndPositions(context.reader().getLiveDocs(), null, DocsAndPositionsEnum.FLAG_PAYLOADS);
            if (uid == null || uid.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                return null; // no doc
            }
            // Note, only master docs uid have version payload, so we can use that info to not
            // take them into account
            do {
                docId = uid.docID();
                uid.nextPosition();
                if (uid.getPayload() == null) {
                    continue;
                }
                if (uid.getPayload().length < 8) {
                    continue;
                }
                byte[] payload = new byte[uid.getPayload().length];
                System.arraycopy(uid.getPayload().bytes, uid.getPayload().offset, payload, 0, uid.getPayload().length);
                return new DocIdAndVersion(docId, Numbers.bytesToLong(payload), context);
            } while (uid.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
            return new DocIdAndVersion(docId, -2, context);
        } catch (Exception e) {
            return new DocIdAndVersion(docId, -2, context);
        }
    }

    /**
     * Load the version for the uid from the reader, returning -1 if no doc exists, or -2 if
     * no version is available (for backward comp.)
     */
    public static long loadVersion(AtomicReaderContext context, Term term) {
        try {
            Terms terms = context.reader().terms(term.field());
            if (terms == null) {
                return -1;
            }
            final TermsEnum termsEnum = terms.iterator(null);
            if (termsEnum == null) {
                return -1;
            }
            if (!termsEnum.seekExact(term.bytes())) {
                return -1;
            }
            DocsAndPositionsEnum uid = termsEnum.docsAndPositions(context.reader().getLiveDocs(), null, DocsAndPositionsEnum.FLAG_PAYLOADS);
            if (uid == null || uid.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                return -1;
            }
            // Note, only master docs uid have version payload, so we can use that info to not
            // take them into account
            do {
                uid.nextPosition();
                if (uid.getPayload() == null) {
                    continue;
                }
                if (uid.getPayload().length < 8) {
                    continue;
                }
                byte[] payload = new byte[uid.getPayload().length];
                System.arraycopy(uid.getPayload().bytes, uid.getPayload().offset, payload, 0, uid.getPayload().length);
                return Numbers.bytesToLong(payload);
            } while (uid.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
            return -2;
        } catch (Exception e) {
            return -2;
        }
    }

    private String uid;

    private long version;

    public UidField(String uid) {
        this(UidFieldMapper.NAME, uid, 0);
    }

    public UidField(String name, String uid, long version) {
        super(name, UidFieldMapper.Defaults.FIELD_TYPE);
        this.uid = uid;
        this.version = version;
        this.tokenStream = new UidPayloadTokenStream(this);
    }

    public String uid() {
        return this.uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String stringValue() {
        return uid;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    public long version() {
        return this.version;
    }

    public void version(long version) {
        this.version = version;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer) throws IOException {
        return tokenStream;
    }

    public static final class UidPayloadTokenStream extends TokenStream {

        private final PayloadAttribute payloadAttribute = addAttribute(PayloadAttribute.class);
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final PositionIncrementAttribute positionIncrementAttr = addAttribute(PositionIncrementAttribute.class);
        

        private final UidField field;

        private boolean added = false;

        public UidPayloadTokenStream(UidField field) {
            this.field = field;
        }

        @Override
        public void reset() throws IOException {
            added = false;
        }
        

        @Override
        public final boolean incrementToken() throws IOException {
            if (added) {
                return false;
            }
            positionIncrementAttr.setPositionIncrement(1); // always set it to 1 since TokenStream#end() will set it to 0 for holes
            termAtt.setLength(0);
            termAtt.append(field.uid);
            payloadAttribute.setPayload(new BytesRef(Numbers.longToBytes(field.version())));
            added = true;
            return true;
        }
    }
}
