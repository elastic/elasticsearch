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
package org.elasticsearch.index.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryMetadataService {

    public static String QUERY_METADATA = "_query_metadata_";
    static String QUERY_METADATA_FIELD_PREFIX = QUERY_METADATA + "field_";
    static String QUERY_METADATA_FIELD_UNKNOWN = QUERY_METADATA + "unknown";
    private static FieldType QUERY_METADATA_FIELD_TYPE = new FieldType();

    static {
        QUERY_METADATA_FIELD_TYPE.setTokenized(false);
        QUERY_METADATA_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        QUERY_METADATA_FIELD_TYPE.freeze();
    }

    public void extractQueryMetadata(Query query, ParseContext.Document document) {
        List<Field> queryMetaDataFields = new ArrayList<>();
        extractQueryMetadata(query, queryMetaDataFields);
        for (Field field : queryMetaDataFields) {
            document.add(field);
        }
    }

    public void extractQueryMetadata(Query query, List<Field> queryMetaDataFields) {
        if (query instanceof TermQuery) {
            Term term = ((TermQuery) query).getTerm();
            addField(term, queryMetaDataFields);
        } else if (query instanceof PhraseQuery) {
            Term[] terms = ((PhraseQuery) query).getTerms();
            for (Term term : terms) {
                addField(term, queryMetaDataFields);
            }
        } else if (query instanceof BooleanQuery) {
            List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
            for (BooleanClause clause : clauses) {
                if (clause.isProhibited()) {
                    // we don't need to remember the things that do *not* match...
                    continue;
                }
                // TODO optimize boolean query with only required clauses
                // in this case only one term needs to be added.
                extractQueryMetadata(clause.getQuery(), queryMetaDataFields);
            }
        } else if (query instanceof ConstantScoreQuery) {
            Query wrappedQuery = ((ConstantScoreQuery) query).getQuery();
            extractQueryMetadata(wrappedQuery, queryMetaDataFields);
        } else {
            queryMetaDataFields.clear();
            queryMetaDataFields.add(new Field(QUERY_METADATA_FIELD_UNKNOWN, new BytesRef(), QUERY_METADATA_FIELD_TYPE));
        }
    }

    public Query createQueryMetadataQuery(Iterable<IndexableField> document, Analyzer indexAnalyzer) throws IOException {
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.add(new TermQuery(new Term(QUERY_METADATA_FIELD_UNKNOWN)), BooleanClause.Occur.SHOULD);

        for (IndexableField field : document) {
            // Ignore none indexed fields
            if (field.fieldType().indexOptions() == IndexOptions.NONE) {
                continue;
            }

            // Ignore meta fields
            if (field.name().startsWith("_")) {
                continue;
            }

            try (TokenStream stream = field.tokenStream(indexAnalyzer, null)) {
                TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
                stream.reset();
                while (stream.incrementToken()) {
                    BytesRef term = BytesRef.deepCopyOf(termAtt.getBytesRef());
                    booleanQuery.add(new TermQuery(new Term(QUERY_METADATA_FIELD_PREFIX + field.name(), term)), BooleanClause.Occur.SHOULD);
                }
                stream.end();
            }
        }
        return booleanQuery.build();
    }

    private static void addField(Term term, List<Field> queryMetaDataFields) {
        queryMetaDataFields.add(new Field(QUERY_METADATA_FIELD_PREFIX + term.field(), term.bytes(), QUERY_METADATA_FIELD_TYPE));
    }

}
