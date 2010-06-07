/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.search.FuzzyLikeThisQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * <pre>
 * {
 *  fuzzy_like_this : {
 *      maxNumTerms : 12,
 *      boost : 1.1,
 *      fields : ["field1", "field2"]
 *      likeText : "..."
 *  }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class FuzzyLikeThisQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "flt";

    public FuzzyLikeThisQueryParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "fuzzy_like_this", "fuzzyLikeThis"};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        int maxNumTerms = 25;
        float boost = 1.0f;
        List<String> fields = null;
        String likeText = null;
        float minSimilarity = 0.5f;
        int prefixLength = 0;
        boolean ignoreTF = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("like_text".equals(currentFieldName) || "likeText".equals(currentFieldName)) {
                    likeText = parser.text();
                } else if ("max_query_terms".equals(currentFieldName) || "maxQueryTerms".equals(currentFieldName)) {
                    maxNumTerms = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("ignore_tf".equals(currentFieldName) || "ignoreTF".equals(currentFieldName)) {
                    ignoreTF = parser.booleanValue();
                } else if ("min_similarity".equals(currentFieldName) || "minSimilarity".equals(currentFieldName)) {
                    minSimilarity = parser.floatValue();
                } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                    prefixLength = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    fields = Lists.newArrayList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parseContext.indexName(parser.text()));
                    }
                }
            }
        }

        if (likeText == null) {
            throw new QueryParsingException(index, "fuzzy_like_this requires 'like_text' to be specified");
        }

        FuzzyLikeThisQuery query = new FuzzyLikeThisQuery(maxNumTerms, parseContext.mapperService().searchAnalyzer());
        if (fields == null) {
            // add the default _all field
            query.addTerms(likeText, AllFieldMapper.NAME, minSimilarity, prefixLength);
        } else {
            for (String field : fields) {
                query.addTerms(likeText, field, minSimilarity, prefixLength);
            }
        }
        query.setBoost(boost);
        query.setIgnoreTF(ignoreTF);

        // move to the next end object, to close the field name
        token = parser.nextToken();
        assert token == XContentParser.Token.END_OBJECT;

        return query;
    }
}