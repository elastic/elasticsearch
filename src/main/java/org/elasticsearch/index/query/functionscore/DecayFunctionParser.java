/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper.GeoStringFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.search.internal.SearchContext;

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
 * one and implement the getDistanceFuntion(). Furthermore, to create a builder,
 * override the getName() in {@link DecayFunctionBuilder}.
 * <p>
 * See {@link GaussDecayFunctionBuilder} and {@link GaussDecayFunctionParser}
 * for an example. The parser furthermore needs to be registered in the
 * {@link org.elasticsearch.index.query.functionscore.FunctionScoreModule
 * FunctionScoreModule}.
 * 
 * **/

public abstract class DecayFunctionParser implements ScoreFunctionParser {

    /**
     * Override this function if you want to produce your own scorer.
     * */
    public abstract DecayFunction getDecayFunction();

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
    public ScoreFunction parse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        String currentFieldName = null;
        XContentParser.Token token;
        ScoreFunction scoreFunction = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                // parse per field the origin and scale value
                scoreFunction = parseVariable(currentFieldName, parser, parseContext);
            } else {
                throw new ElasticSearchParseException("Malformed score function score parameters.");
            }
        } else {
            throw new ElasticSearchParseException("Malformed score function score parameters.");
        }
        parser.nextToken();
        return scoreFunction;
    }

    // parses origin and scale parameter for field "fieldName"
    private ScoreFunction parseVariable(String fieldName, XContentParser parser, QueryParseContext parseContext) throws IOException {

        // now, the field must exist, else we cannot read the value for
        // the doc later
        MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "Unknown field [" + fieldName + "]");
        }

        FieldMapper<?> mapper = smartMappers.fieldMappers().mapper();
        // dates and time need special handling
        if (mapper instanceof DateFieldMapper) {
            return parseDateVariable(fieldName, parser, parseContext, (DateFieldMapper) mapper);
        } else if (mapper instanceof GeoStringFieldMapper) {
            return parseGeoVariable(fieldName, parser, parseContext, (GeoStringFieldMapper) mapper);
        } else if (mapper instanceof NumberFieldMapper<?>) {
            return parseNumberVariable(fieldName, parser, parseContext, (NumberFieldMapper<?>) mapper);
        } else {
            throw new QueryParsingException(parseContext.index(), "Field " + fieldName + " is of type " + mapper.fieldType()
                    + ", but only numeric types are supported.");
        }
    }

    private ScoreFunction parseNumberVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            NumberFieldMapper<?> mapper) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        double scale = 0;
        double origin = 0;
        double decay = 0.5;
        double offset = 0.0d;
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
                throw new ElasticSearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        if (!scaleFound || !refFound) {
            throw new ElasticSearchParseException("Both " + DecayFunctionBuilder.SCALE + "and " + DecayFunctionBuilder.ORIGIN
                    + " must be set for numeric fields.");
        }
        IndexNumericFieldData<?> numericFieldData = parseContext.fieldData().getForField(mapper);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData);
    }

    private ScoreFunction parseGeoVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            GeoStringFieldMapper mapper) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        GeoPoint origin = new GeoPoint();
        String scaleString = null;
        String offsetString = "0km";
        double decay = 0.5;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals(DecayFunctionBuilder.SCALE)) {
                scaleString = parser.text();
            } else if (parameterName.equals(DecayFunctionBuilder.ORIGIN)) {
                origin = GeoPoint.parse(parser);
            } else if (parameterName.equals(DecayFunctionBuilder.DECAY)) {
                decay = parser.doubleValue();
            } else if (parameterName.equals(DecayFunctionBuilder.OFFSET)) {
                offsetString = parser.text();
            } else {
                throw new ElasticSearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        if (origin == null || scaleString == null) {
            throw new ElasticSearchParseException(DecayFunctionBuilder.ORIGIN + " and " + DecayFunctionBuilder.SCALE + " must be set for geo fields.");
        }
        double scale = DistanceUnit.parse(scaleString, DistanceUnit.METERS, DistanceUnit.METERS);
        double offset = DistanceUnit.parse(offsetString, DistanceUnit.METERS, DistanceUnit.METERS);
        IndexGeoPointFieldData<?> indexFieldData = parseContext.fieldData().getForField(mapper);
        return new GeoFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), indexFieldData);

    }

    private ScoreFunction parseDateVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            DateFieldMapper dateFieldMapper) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        String scaleString = null;
        String originString = null;
        String offsetString = "0d";
        double decay = 0.5;
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
                throw new ElasticSearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        long origin = SearchContext.current().nowInMillis();
        if (originString != null) {
            origin = dateFieldMapper.parseToMilliseconds(originString, parseContext);
        }

        if (scaleString == null) {
            throw new ElasticSearchParseException(DecayFunctionBuilder.SCALE + " must be set for date fields.");
        }
        TimeValue val = TimeValue.parseTimeValue(scaleString, TimeValue.timeValueHours(24));
        double scale = val.getMillis();
        val = TimeValue.parseTimeValue(offsetString, TimeValue.timeValueHours(24));
        double offset = val.getMillis();
        IndexNumericFieldData<?> numericFieldData = parseContext.fieldData().getForField(dateFieldMapper);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData);
    }

    static class GeoFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        private final GeoPoint origin;
        private final IndexGeoPointFieldData<?> fieldData;
        private GeoPointValues geoPointValues = null;

        private static final GeoDistance distFunction = GeoDistance.fromString("arc");

        public GeoFieldDataScoreFunction(GeoPoint origin, double scale, double decay, double offset, DecayFunction func,
                IndexGeoPointFieldData<?> fieldData) {
            super(scale, decay, offset, func);
            this.origin = origin;
            this.fieldData = fieldData;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            geoPointValues = fieldData.load(context).getGeoPointValues();
        }

        private final GeoPoint getValue(int doc, GeoPoint missing) {
            final int num = geoPointValues.setDocument(doc);
            for (int i = 0; i < num; i++) {
                return geoPointValues.nextValue();
            }
            return missing;
        }

        @Override
        protected double distance(int docId) {
            GeoPoint other = getValue(docId, origin);
            double distance = Math.abs(distFunction.calculate(origin.lat(), origin.lon(), other.lat(), other.lon(),
                    DistanceUnit.METERS)) - offset;
            return Math.max(0.0d, distance);
        }

        @Override
        protected String getDistanceString(int docId) {
            final GeoPoint other = getValue(docId, origin);
            return "arcDistance(" + other + "(=doc value), " + origin + "(=origin)) - " + offset
                    + "(=offset) < 0.0 ? 0.0: arcDistance(" + other + "(=doc value), " + origin + "(=origin)) - " + offset
                    + "(=offset)";
        }

        @Override
        protected String getFieldName() {
            return fieldData.getFieldNames().fullName();
        }
    }

    static class NumericFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        private final IndexNumericFieldData<?> fieldData;
        private final double origin;
        private DoubleValues doubleValues;

        public NumericFieldDataScoreFunction(double origin, double scale, double decay, double offset, DecayFunction func,
                IndexNumericFieldData<?> fieldData) {
            super(scale, decay, offset, func);
            this.fieldData = fieldData;
            this.origin = origin;
        }

        public void setNextReader(AtomicReaderContext context) {
            this.doubleValues = this.fieldData.load(context).getDoubleValues();
        }

        private final double getValue(int doc, double missing) {
            final int num = doubleValues.setDocument(doc);
            for (int i = 0; i < num; i++) {
                return doubleValues.nextValue();
            }
            return missing;
        }

        @Override
        protected double distance(int docId) {
            double distance = Math.abs(getValue(docId, origin) - origin) - offset;
            return Math.max(0.0d, distance);
        }

        @Override
        protected String getDistanceString(int docId) {
            return "Math.abs(" + getValue(docId, origin) + "(=doc value) - " + origin + "(=origin)) - "
                    + offset + "(=offset) < 0.0 ? 0.0: Math.abs(" + getValue(docId, origin) + "(=doc value) - "
                    + origin + ") - " + offset + "(=offset)";
        }

        @Override
        protected String getFieldName() {
            return fieldData.getFieldNames().fullName();
        }
    }

    /**
     * This is the base class for scoring a single field.
     * 
     * */
    public static abstract class AbstractDistanceScoreFunction extends ScoreFunction {

        private final double scale;
        protected final double offset;
        private final DecayFunction func;

        public AbstractDistanceScoreFunction(double userSuppiedScale, double decay, double offset, DecayFunction func) {
            super(CombineFunction.MULT);
            if (userSuppiedScale <= 0.0) {
                throw new ElasticSearchIllegalArgumentException(FunctionScoreQueryParser.NAME + " : scale must be > 0.0.");
            }
            if (decay <= 0.0 || decay >= 1.0) {
                throw new ElasticSearchIllegalArgumentException(FunctionScoreQueryParser.NAME
                        + " : decay must be in the range [0..1].");
            }
            this.scale = func.processScale(userSuppiedScale, decay);
            this.func = func;
            if (offset < 0.0d) {
                throw new ElasticSearchIllegalArgumentException(FunctionScoreQueryParser.NAME + " : offset must be > 0.0");
            }
            this.offset = offset;
        }

        @Override
        public double score(int docId, float subQueryScore) {
            double value = distance(docId);
            return func.evaluate(value, scale);
        }

        /**
         * This function computes the distance from a defined origin. Since
         * the value of the document is read from the index, it cannot be
         * guaranteed that the value actually exists. If it does not, we assume
         * the user handles this case in the query and return 0.
         * */
        protected abstract double distance(int docId);

        protected abstract String getDistanceString(int docId);

        protected abstract String getFieldName();

        @Override
        public Explanation explainScore(int docId, Explanation subQueryExpl) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setValue(CombineFunction.toFloat(score(docId, subQueryExpl.getValue())));
            ce.setMatch(true);
            ce.setDescription("Function for field " + getFieldName() + ":");
            ce.addDetail(func.explainFunction(getDistanceString(docId), distance(docId), scale));
            return ce;
        }

    }

}
