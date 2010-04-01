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

package org.elasticsearch.index.query.json;

import com.google.common.collect.Lists;
import org.apache.lucene.search.FuzzyLikeThisQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Booleans;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.List;

/**
 * <pre>
 * {
 *  fuzzyLikeThis : {
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
public class FuzzyLikeThisJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "fuzzyLikeThis";

    public FuzzyLikeThisJsonQueryParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        int maxNumTerms = 100;
        float boost = 1.0f;
        List<String> fields = null;
        String likeText = null;
        float minSimilarity = 0.5f;
        int prefixLength = 0;
        boolean ignoreTF = false;

        JsonToken token;
        String currentFieldName = null;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_STRING) {
                if ("likeText".equals(currentFieldName)) {
                    likeText = jp.getText();
                } else if ("maxNumTerms".equals(currentFieldName)) {
                    maxNumTerms = Integer.parseInt(jp.getText());
                } else if ("boost".equals(currentFieldName)) {
                    boost = Float.parseFloat(jp.getText());
                } else if ("ignoreTF".equals(currentFieldName)) {
                    ignoreTF = Booleans.parseBoolean(jp.getText(), false);
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                if ("maxNumTerms".equals(currentFieldName)) {
                    maxNumTerms = jp.getIntValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = jp.getIntValue();
                } else if ("ignoreTF".equals(currentFieldName)) {
                    ignoreTF = jp.getIntValue() != 0;
                }
            } else if (token == JsonToken.VALUE_TRUE) {
                if ("ignoreTF".equals(currentFieldName)) {
                    ignoreTF = true;
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                }
            } else if (token == JsonToken.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    fields = Lists.newArrayList();
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        fields.add(parseContext.indexName(jp.getText()));
                    }
                }
            }
        }

        if (likeText == null) {
            throw new QueryParsingException(index, "fuzzyLikeThis requires 'likeText' to be specified");
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
        token = jp.nextToken();
        assert token == JsonToken.END_OBJECT;

        return query;
    }
}