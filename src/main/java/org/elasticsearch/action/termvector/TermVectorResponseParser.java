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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
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

    public class ParsedTermVectorResponse {

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
        MemoryIndex index = new MemoryIndex();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (parser.currentName().equals("field_statistics")) {
                        parser.skipChildren(); // not implemented
                    } else if (parser.currentName().equals("terms")) {
                        parser.nextToken();
                        addToMemoryIndex(index, currentFieldName, parser.map());
                    }
                }
            }
        }
        return MultiFields.getFields(index.createSearcher().getIndexReader());
    }

    private void addToMemoryIndex(MemoryIndex index, String fieldName, Map<String, Object> termInfo) throws IOException {
        for (Map.Entry<String, Object> term : termInfo.entrySet()) {
            int termFreq = (int) ((Map) (term.getValue())).get("term_freq");
            for (int i = 0; i < termFreq; i++) {
                index.addField(fieldName, term.getKey(), new KeywordAnalyzer());
            }
        }
    }
}

