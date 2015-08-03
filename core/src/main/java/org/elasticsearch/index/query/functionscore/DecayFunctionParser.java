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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;

import java.io.IOException;

/**
 * This class provides the basic functionality needed for adding a decay
 * function.
 *
 * This parser parses this kind of input
 *
 * <pre>
 * {@code}
 * {
 *      "fieldname1" : {
 *          "origin" = "someValue",
 *          "scale" = "someValue"
 *      }
 *
 * }
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
 * one and implement the getDistanceFunction(). Furthermore, to create a builder,
 * override the getName() in {@link DecayFunctionBuilder}.
 * <p>
 * See {@link GaussDecayFunctionBuilder} and {@link GaussDecayFunctionParser}
 * for an example. The parser furthermore needs to be registered in the
 * {@link org.elasticsearch.index.query.functionscore.FunctionScoreModule
 * FunctionScoreModule}.
 *
 * **/

public abstract class DecayFunctionParser implements ScoreFunctionParser {

    public static final ParseField MULTI_VALUE_MODE = new ParseField("multi_value_mode");

    /**
     * Override this function if you want to produce your own scorer.
     */
    public abstract DecayFunctionBuilder newDecayFunctionBuilder(String fieldName, Object origin, Object scale);

    /**
     * Parses bodies of the kind
     *
     * <pre>
     * {@code}
     * {
     *      "fieldname1" : {
     *          "origin" = "someValue",
     *          "scale" = "someValue"
     *      }
     *
     * }
     * </pre>
     *
     * */
    @Override
    public ScoreFunctionBuilder fromXContent(QueryParseContext context, XContentParser parser) throws IOException, QueryParsingException {
        String currentFieldName;
        XContentParser.Token token;
        ScoreFunctionBuilder scoreFunction;
        String multiValueMode = DecayFunctionBuilder.DEFAULT_MULTI_VALUE_MODE.name();
        XContentBuilder variableContent = XContentFactory.jsonBuilder();
        String fieldName = null;
        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                variableContent.copyCurrentStructure(parser);
                fieldName = currentFieldName;
            } else if (context.parseFieldMatcher().match(currentFieldName, MULTI_VALUE_MODE)) {
                multiValueMode = parser.text();
            } else {
                throw new ElasticsearchParseException("malformed score function score parameters.");
            }
        }
        if (fieldName == null) {
            throw new ElasticsearchParseException("malformed score function score parameters.");
        }
        XContentParser variableParser = XContentFactory.xContent(variableContent.string()).createParser(variableContent.string());
        scoreFunction = parseVariable(fieldName, variableParser, context, multiValueMode);
        return scoreFunction;
    }

    // parses origin and scale parameter for field "fieldName"
    private ScoreFunctionBuilder parseVariable(String fieldName, XContentParser parser, QueryParseContext context, String mode) throws IOException {
        // now, the field must exist, else we cannot read the value for
        // the doc later
        //NO COMMIT: parsing here is field type dependant!
        MappedFieldType fieldType = context.shardContext().fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryParsingException(context, "unknown field [{}]", fieldName);
        }

        // dates and time need special handling
        parser.nextToken();
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            return parseDateVariable(fieldName, parser, mode);
        } else if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType) {
            return parseGeoVariable(fieldName, parser, mode);
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return parseNumberVariable(fieldName, parser, mode);
        } else {
            throw new QueryParsingException(context, "field [{}] is of type [{}], but only numeric types are supported.", fieldName, fieldType);
        }
    }

    private ScoreFunctionBuilder parseNumberVariable(String fieldName, XContentParser parser, String mode) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        double scale = 0;
        double origin = 0;
        double decay = DecayFunctionBuilder.DEFAULT_DECAY;
        double offset = DecayFunctionBuilder.DEFAULT_OFFSET_NUMBER;
        boolean scaleFound = false;
        boolean refFound = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals(DecayFunctionBuilder.SCALE)) {
                scale = parser.doubleValue();
                scaleFound = true;
            } else if (parameterName.equals(DecayFunctionBuilder.DECAY)) {
                decay = parser.doubleValue();
            } else if (parameterName.equals(DecayFunctionBuilder.ORIGIN)) {
                origin = parser.doubleValue();
                refFound = true;
            } else if (parameterName.equals(DecayFunctionBuilder.OFFSET)) {
                offset = parser.doubleValue();
            } else {
                throw new ElasticsearchParseException("parameter [{}] not supported!", parameterName);
            }
        }
        if (!scaleFound || !refFound) {
            throw new ElasticsearchParseException("both [{}] and [{}] must be set for numeric fields.", DecayFunctionBuilder.SCALE, DecayFunctionBuilder.ORIGIN);
        }
        return newDecayFunctionBuilder(fieldName, origin, scale)
                .setDecay(decay)
                .setOffset(offset)
                .setMultiValueMode(mode);
    }

    private ScoreFunctionBuilder parseGeoVariable(String fieldName, XContentParser parser, String mode) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        GeoPoint origin = new GeoPoint();
        String scaleString = null;
        String offsetString = DecayFunctionBuilder.DEFAULT_OFFSET_GEO;
        double decay = DecayFunctionBuilder.DEFAULT_DECAY;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals(DecayFunctionBuilder.SCALE)) {
                scaleString = parser.text();
            } else if (parameterName.equals(DecayFunctionBuilder.ORIGIN)) {
                origin = GeoUtils.parseGeoPoint(parser);
            } else if (parameterName.equals(DecayFunctionBuilder.DECAY)) {
                decay = parser.doubleValue();
            } else if (parameterName.equals(DecayFunctionBuilder.OFFSET)) {
                offsetString = parser.text();
            } else {
                throw new ElasticsearchParseException("parameter [{}] not supported!", parameterName);
            }
        }
        if (origin == null || scaleString == null) {
            throw new ElasticsearchParseException("[{}] and [{}] must be set for geo fields.", DecayFunctionBuilder.ORIGIN, DecayFunctionBuilder.SCALE);
        }
        return newDecayFunctionBuilder(fieldName, origin, scaleString)
                .setDecay(decay)
                .setOffset(offsetString)
                .setMultiValueMode(mode);
    }

    private ScoreFunctionBuilder parseDateVariable(String fieldName, XContentParser parser, String mode) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        String scaleString = null;
        String originString = null;
        String offsetString = DecayFunctionBuilder.DEFAULT_OFFSET_DATE;
        double decay = DecayFunctionBuilder.DEFAULT_DECAY;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals(DecayFunctionBuilder.SCALE)) {
                scaleString = parser.text();
            } else if (parameterName.equals(DecayFunctionBuilder.ORIGIN)) {
                originString = parser.text();
            } else if (parameterName.equals(DecayFunctionBuilder.DECAY)) {
                decay = parser.doubleValue();
            } else if (parameterName.equals(DecayFunctionBuilder.OFFSET)) {
                offsetString = parser.text();
            } else {
                throw new ElasticsearchParseException("parameter [{}] not supported!", parameterName);
            }
        }
        if (scaleString == null) {
            throw new ElasticsearchParseException("[{}] must be set for date fields.", DecayFunctionBuilder.SCALE);
        }
        return newDecayFunctionBuilder(fieldName, originString, scaleString)
                .setDecay(decay)
                .setOffset(offsetString)
                .setMultiValueMode(mode);
    }

    @Override
    public DecayFunctionBuilder getBuilderPrototype() {
        return newDecayFunctionBuilder(null, null, null);
    }
}
