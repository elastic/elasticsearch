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


package org.elasticsearch.index.query.functionscore.random;


import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public class RandomScoreFunctionParser implements ScoreFunctionParser {

    public static String[] NAMES = { "random_score", "randomScore" };

    @Inject
    public RandomScoreFunctionParser() {
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }

    @Override
    public ScoreFunction parse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {

        int seed = -1;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("seed".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (parser.numberType() == XContentParser.NumberType.INT) {
                            seed = parser.intValue();
                        } else if (parser.numberType() == XContentParser.NumberType.LONG) {
                            seed = hash(parser.longValue());
                        } else {
                            throw new QueryParsingException(parseContext, "random_score seed must be an int, long or string, not '"
                                    + token.toString() + "'");
                        }
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        seed = parser.text().hashCode();
                    } else {
                        throw new QueryParsingException(parseContext, "random_score seed must be an int/long or string, not '"
                                + token.toString() + "'");
                    }
                } else {
                    throw new QueryParsingException(parseContext, NAMES[0] + " query does not support [" + currentFieldName + "]");
                }
            }
        }

        final MappedFieldType fieldType = SearchContext.current().mapperService().smartNameFieldType("_uid");
        if (fieldType == null) {
            // mapper could be null if we are on a shard with no docs yet, so this won't actually be used
            return new RandomScoreFunction();
        }

        if (seed == -1) {
            seed = hash(parseContext.nowInMillis());
        }
        final ShardId shardId = SearchContext.current().indexShard().shardId();
        final int salt = (shardId.index().name().hashCode() << 10) | shardId.id();
        final IndexFieldData<?> uidFieldData = SearchContext.current().fieldData().getForField(fieldType);

        return new RandomScoreFunction(seed, salt, uidFieldData);
    }

    private static final int hash(long value) {
        return (int) (value ^ (value >>> 32));
    }
}