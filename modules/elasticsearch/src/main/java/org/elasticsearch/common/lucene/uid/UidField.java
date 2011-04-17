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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.AbstractField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.io.Reader;

/**
 * @author kimchy (shay.banon)
 */
public class UidField extends AbstractField {

    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final IndexReader reader;

        public DocIdAndVersion(int docId, long version, IndexReader reader) {
            this.docId = docId;
            this.version = version;
            this.reader = reader;
        }
    }

    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, Term term) {
        int docId = Lucene.NO_DOC;
        TermPositions uid = null;
        try {
            uid = reader.termPositions(term);
            if (!uid.next()) {
                return null; // no doc
            }
            docId = uid.doc();
            uid.nextPosition();
            if (!uid.isPayloadAvailable()) {
                return new DocIdAndVersion(docId, -2, reader);
            }
            if (uid.getPayloadLength() < 8) {
                return new DocIdAndVersion(docId, -2, reader);
            }
            byte[] payload = uid.getPayload(new byte[8], 0);
            return new DocIdAndVersion(docId, Numbers.bytesToLong(payload), reader);
        } catch (Exception e) {
            return new DocIdAndVersion(docId, -2, reader);
        } finally {
            if (uid != null) {
                try {
                    uid.close();
                } catch (IOException e) {
                    // nothing to do here...
                }
            }
        }
    }

    /**
     * Load the version for the uid from the reader, returning -1 if no doc exists, or -2 if
     * no version is available (for backward comp.)
     */
    public static long loadVersion(IndexReader reader, Term term) {
        TermPositions uid = null;
        try {
            uid = reader.termPositions(term);
            if (!uid.next()) {
                return -1;
            }
            uid.nextPosition();
            if (!uid.isPayloadAvailable()) {
                return -2;
            }
            if (uid.getPayloadLength() < 8) {
                return -2;
            }
            byte[] payload = uid.getPayload(new byte[8], 0);
            return Numbers.bytesToLong(payload);
        } catch (Exception e) {
            return -2;
        } finally {
            if (uid != null) {
                try {
                    uid.close();
                } catch (IOException e) {
                    // nothing to do here...
                }
            }
        }
    }

    private String uid;

    private long version;

    private final UidPayloadTokenStream tokenStream;

    public UidField(String name, String uid, long version) {
        super(name, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO);
        this.uid = uid;
        this.version = version;
        this.omitTermFreqAndPositions = false;
        this.tokenStream = new UidPayloadTokenStream(this);
    }

    @Override public void setOmitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
        // never allow to set this, since we want payload!
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override public String stringValue() {
        return uid;
    }

    @Override public Reader readerValue() {
        return null;
    }

    public long version() {
        return this.version;
    }

    public void version(long version) {
        this.version = version;
    }

    @Override public TokenStream tokenStreamValue() {
        return tokenStream;
    }

    public static final class UidPayloadTokenStream extends TokenStream {

        private final PayloadAttribute payloadAttribute = addAttribute(PayloadAttribute.class);
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        private final UidField field;

        private boolean added = false;

        public UidPayloadTokenStream(UidField field) {
            this.field = field;
        }

        @Override public void reset() throws IOException {
            added = false;
        }

        @Override public final boolean incrementToken() throws IOException {
            if (added) {
                return false;
            }
            termAtt.setLength(0);
            termAtt.append(field.uid);
            payloadAttribute.setPayload(new Payload(Numbers.longToBytes(field.version())));
            added = true;
            return true;
        }
    }
}
