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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * This class provides the basic functionality needed for adding a decay
 * function.
 *
 * This parser parses this kind of input
 *
 * <pre>
 * <code>
 * {
 *      "fieldname1" : {
 *          "origin" = "someValue",
 *          "scale" = "someValue"
 *      },
 *      "multi_value_mode" : "min"
 * }
 * </code>
 * </pre>
 *
 * "origin" here refers to the reference point and "scale" to the level of
 * uncertainty you have in your origin.
 * <p>
 *
 * For example, you might want to retrieve an event that took place around the
 * 20 May 2010 somewhere near Berlin. You are mainly interested in events that
 * are close to the 20 May 2010 but you are unsure about your guess, maybe it
 * was a week before or after that. Your "origin" for the date field would be
 * "20 May 2010" and your "scale" would be "7d".
 *
 * This class parses the input and creates a scoring function from the
 * parameters origin and scale.
 * <p>
 * To write a new scoring function, create a new class that inherits from this
 * one and implements {@link #getBuilderPrototype()} and {@link #getNames()}.
 * Also create its corresponding {@link DecayFunctionBuilder}. The latter needs to
 * implement {@link DecayFunctionBuilder#doReadFrom(StreamInput)} and
 * {@link DecayFunctionBuilder#doWriteTo(StreamOutput)} for serialization purposes,
 * {@link DecayFunctionBuilder#doEquals(DecayFunctionBuilder)} and
 * {@link DecayFunctionBuilder#doHashCode()} for equality checks,
 * {@link DecayFunctionBuilder#getName()} that returns the name of the function and
 * {@link DecayFunctionBuilder#getDecayFunction()} which returns the corresponding lucene function.
 * <p>
 * See {@link GaussDecayFunctionBuilder} and {@link GaussDecayFunctionParser}
 * for an example. The parser furthermore needs to be registered in the
 * {@link org.elasticsearch.search.SearchModule SearchModule}.
 *
 */

public abstract class DecayFunctionParser<DFB extends DecayFunctionBuilder<DFB>> implements ScoreFunctionParser<DFB> {

    public static final ParseField MULTI_VALUE_MODE = new ParseField("multi_value_mode");

    /**
     * Parses bodies of the kind
     *
     * <pre>
     * <code>
     * {
     *      "fieldname1" : {
     *          "origin" : "someValue",
     *          "scale" : "someValue"
     *      },
     *      "multi_value_mode" : "min"
     * }
     * </code>
     * </pre>
     */
    @Override
    public DFB fromXContent(QueryParseContext context, XContentParser parser) throws IOException, ParsingException {
        String currentFieldName;
        XContentParser.Token token;
        MultiValueMode multiValueMode = DecayFunctionBuilder.DEFAULT_MULTI_VALUE_MODE;
        String fieldName = null;
        BytesReference functionBytes = null;
        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.copyCurrentStructure(parser);
                functionBytes = builder.bytes();
            } else if (context.parseFieldMatcher().match(currentFieldName, MULTI_VALUE_MODE)) {
                multiValueMode = MultiValueMode.fromString(parser.text());
            } else {
                throw new ParsingException(parser.getTokenLocation(), "malformed score function score parameters.");
            }
        }
        if (fieldName == null || functionBytes == null) {
            throw new ParsingException(parser.getTokenLocation(), "malformed score function score parameters.");
        }
        DFB functionBuilder = getBuilderPrototype().createFunctionBuilder(fieldName, functionBytes);
        functionBuilder.setMultiValueMode(multiValueMode);
        return functionBuilder;
    }
}
