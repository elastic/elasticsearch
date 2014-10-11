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

package org.elasticsearch.action.termvector;

import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.termvector.TermVectorFields.TermVectorDocsAndPosEnum;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class is meant to parse the JSON response of a {@link TermVectorResponse} so that term vectors
 * could be passed from {@link org.elasticsearch.action.mlt.TransportMoreLikeThisAction}
 * to {@link org.elasticsearch.index.query.MoreLikeThisQueryParser}.
 *
 * <p>
 * At the moment only <em>_index</em>, <em>_type</em>, <em>_id</em> and <em>term_vectors</em> are
 * parsed from the response. Term vectors are returned as a {@link Fields} object.
 * </p>
*/
public class TermVectorResponseParser {

    public static class ParsedTermVectorResponse {

        private final String index;

        private final String type;

        private final String id;

        private final Fields termVectorFields;

        public ParsedTermVectorResponse(String index, String type, String id, Fields termVectorResponseFields) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.termVectorFields = termVectorResponseFields;
        }

        public String index() {
            return index;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public Fields termVectorFields() {
            return termVectorFields;
        }
    }

    private XContentParser parser;

    public TermVectorResponseParser(XContentParser parser) throws IOException {
        this.parser = parser;
    }

    public ParsedTermVectorResponse parse() throws IOException {
        String index = null;
        String type = null;
        String id = null;
        Fields termVectorFields = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName != null) {
                if (currentFieldName.equals("_index")) {
                    index = parser.text();
                } else if (currentFieldName.equals("_type")) {
                    type = parser.text();
                } else if (currentFieldName.equals("_id")) {
                    id = parser.text();
                } else if (currentFieldName.equals("term_vectors")) {
                    termVectorFields = parseTermVectors();
                }
            }
        }
        if (index == null || type == null || id == null || termVectorFields == null) {
            throw new ElasticsearchParseException("\"_index\", \"_type\", \"_id\" or \"term_vectors\" missing from the response!");
        }
        return new ParsedTermVectorResponse(index, type, id, termVectorFields);
    }

    private Fields parseTermVectors() throws IOException {
        Map<String, Terms> termVectors = new HashMap<>();
        XContentParser.Token token;
        String currentFieldName;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                Map<String, Object> terms = null;
                Map<String, Object> fieldStatistics = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (parser.currentName().equals("terms")) {
                        parser.nextToken();
                        terms = parser.map();
                    }
                    if (parser.currentName().equals("field_statistics")) {
                        parser.nextToken();
                        fieldStatistics = parser.map();
                    }
                }
                if (terms != null) {
                    termVectors.put(currentFieldName, makeTermVector(terms, fieldStatistics));
                }
            }
        }
        return makeTermVectors(termVectors);
    }

    private Terms makeTermVector(final Map<String, Object> terms, final Map<String, Object> fieldStatistics) {
        return new Terms() {
            @Override
            public TermsEnum iterator(TermsEnum reuse) throws IOException {
                return makeTermsEnum(terms);
            }

            @Override
            public Comparator<BytesRef> getComparator() {
                return BytesRef.getUTF8SortedAsUnicodeComparator();
            }

            @Override
            public long size() throws IOException {
                return terms.size();
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                return fieldStatistics != null ? (long) fieldStatistics.get("sum_ttf") : -1;
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return fieldStatistics != null ? (long) fieldStatistics.get("sum_doc_freq") : -1;
            }

            @Override
            public int getDocCount() throws IOException {
                return fieldStatistics != null ? (int) fieldStatistics.get("doc_count") : -1;
            }

            @Override
            public boolean hasFreqs() {
                return true;
            }

            @Override
            public boolean hasOffsets() {
                return false;
            }

            @Override
            public boolean hasPositions() {
                return false;
            }

            @Override
            public boolean hasPayloads() {
                return false;
            }
        };
    }

    private TermsEnum makeTermsEnum(final Map<String, Object> terms) {
        final Iterator<String> iterator = terms.keySet().iterator();
        return new TermsEnum() {
            BytesRef currentTerm;
            int termFreq = -1;
            int docFreq = -1;
            long totalTermFreq = -1;

            @Override
            public BytesRef next() throws IOException {
                if (iterator.hasNext()) {
                    String term = iterator.next();
                    setTermStats(term);
                    currentTerm = new BytesRef(term);
                    return currentTerm;
                } else {
                    return null;
                }
            }

            private void setTermStats(String term) {
                // we omit positions, offsets and payloads
                Map<String, Object> termStats = (Map<String, Object>) terms.get(term);
                termFreq = (int) termStats.get("term_freq");
                if (termStats.containsKey("doc_freq")) {
                    docFreq = (int) termStats.get("doc_freq");
                }
                if (termStats.containsKey("total_term_freq")) {
                    totalTermFreq = (int) termStats.get("total_term_freq");
                }
            }

            @Override
            public BytesRef term() throws IOException {
                return currentTerm;
            }

            @Override
            public SeekStatus seekCeil(BytesRef text) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void seekExact(long ord) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long ord() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int docFreq() throws IOException {
                return docFreq;
            }

            @Override
            public long totalTermFreq() throws IOException {
                return totalTermFreq;
            }

            @Override
            public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
                return docsAndPositions(liveDocs, reuse instanceof DocsAndPositionsEnum ? (DocsAndPositionsEnum) reuse : null, 0);
            }

            @Override
            public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
                final TermVectorDocsAndPosEnum retVal = reuse instanceof TermVectorDocsAndPosEnum ? (TermVectorDocsAndPosEnum) reuse
                        : new TermVectorDocsAndPosEnum();
                return retVal.reset(null, null, null, null, termFreq);  // only care about term freq
            }

            @Override
            public Comparator<BytesRef> getComparator() {
                return BytesRef.getUTF8SortedAsUnicodeComparator();
            }
        };
    }

    private Fields makeTermVectors(final Map<String, Terms> termVectors) {
        return new Fields() {
            @Override
            public Iterator<String> iterator() {
                return termVectors.keySet().iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                return termVectors.get(field);
            }

            @Override
            public int size() {
                return termVectors.size();
            }
        };
    }
}

