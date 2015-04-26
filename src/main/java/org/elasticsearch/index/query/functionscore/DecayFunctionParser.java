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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Locale;

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
        String currentFieldName;
        XContentParser.Token token;
        AbstractDistanceScoreFunction scoreFunction = null;
        String multiValueMode = "MIN";
        XContentBuilder variableContent = XContentFactory.jsonBuilder();
        String fieldName = null;
        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                variableContent.copyCurrentStructure(parser);
                fieldName = currentFieldName;
            } else if (MULTI_VALUE_MODE.match(currentFieldName)) {
                multiValueMode = parser.text();
            } else {
                throw new ElasticsearchParseException("Malformed score function score parameters.");
            }
        }
        if (fieldName == null) {
            throw new ElasticsearchParseException("Malformed score function score parameters.");
        }
        XContentParser variableParser = XContentFactory.xContent(variableContent.string()).createParser(variableContent.string());
        scoreFunction = parseVariable(fieldName, variableParser, parseContext, MultiValueMode.fromString(multiValueMode.toUpperCase(Locale.ROOT)));
        return scoreFunction;
    }

    // parses origin and scale parameter for field "fieldName"
    private AbstractDistanceScoreFunction parseVariable(String fieldName, XContentParser parser, QueryParseContext parseContext, MultiValueMode mode) throws IOException {

        // now, the field must exist, else we cannot read the value for
        // the doc later
        MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "Unknown field [" + fieldName + "]");
        }

        FieldMapper<?> mapper = smartMappers.fieldMappers().mapper();
        // dates and time need special handling
        parser.nextToken();
        if (mapper instanceof DateFieldMapper) {
            return parseDateVariable(fieldName, parser, parseContext, (DateFieldMapper) mapper, mode);
        } else if (mapper instanceof GeoPointFieldMapper) {
            return parseGeoVariable(fieldName, parser, parseContext, (GeoPointFieldMapper) mapper, mode);
        } else if (mapper instanceof NumberFieldMapper<?>) {
            return parseNumberVariable(fieldName, parser, parseContext, (NumberFieldMapper<?>) mapper, mode);
        } else {
            throw new QueryParsingException(parseContext.index(), "Field " + fieldName + " is of type " + mapper.fieldType()
                    + ", but only numeric types are supported.");
        }
    }

    private AbstractDistanceScoreFunction parseNumberVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            NumberFieldMapper<?> mapper, MultiValueMode mode) throws IOException {
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
                throw new ElasticsearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        if (!scaleFound || !refFound) {
            throw new ElasticsearchParseException("Both " + DecayFunctionBuilder.SCALE + " and " + DecayFunctionBuilder.ORIGIN
                    + " must be set for numeric fields.");
        }
        IndexNumericFieldData numericFieldData = parseContext.getForField(mapper);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData, mode);
    }

    private AbstractDistanceScoreFunction parseGeoVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            GeoPointFieldMapper mapper, MultiValueMode mode) throws IOException {
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
                origin = GeoUtils.parseGeoPoint(parser);
            } else if (parameterName.equals(DecayFunctionBuilder.DECAY)) {
                decay = parser.doubleValue();
            } else if (parameterName.equals(DecayFunctionBuilder.OFFSET)) {
                offsetString = parser.text();
            } else {
                throw new ElasticsearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        if (origin == null || scaleString == null) {
            throw new ElasticsearchParseException(DecayFunctionBuilder.ORIGIN + " and " + DecayFunctionBuilder.SCALE + " must be set for geo fields.");
        }
        double scale = DistanceUnit.DEFAULT.parse(scaleString, DistanceUnit.DEFAULT);
        double offset = DistanceUnit.DEFAULT.parse(offsetString, DistanceUnit.DEFAULT);
        IndexGeoPointFieldData indexFieldData = parseContext.getForField(mapper);
        return new GeoFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), indexFieldData, mode);

    }

    private AbstractDistanceScoreFunction parseDateVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            DateFieldMapper dateFieldMapper, MultiValueMode mode) throws IOException {
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
                throw new ElasticsearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        long origin = SearchContext.current().nowInMillis();
        if (originString != null) {
            origin = dateFieldMapper.parseToMilliseconds(originString);
        }

        if (scaleString == null) {
            throw new ElasticsearchParseException(DecayFunctionBuilder.SCALE + " must be set for date fields.");
        }
        TimeValue val = TimeValue.parseTimeValue(scaleString, TimeValue.timeValueHours(24));
        double scale = val.getMillis();
        val = TimeValue.parseTimeValue(offsetString, TimeValue.timeValueHours(24));
        double offset = val.getMillis();
        IndexNumericFieldData numericFieldData = parseContext.getForField(dateFieldMapper);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData, mode);
    }

    static class GeoFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        private final GeoPoint origin;
        private final IndexGeoPointFieldData fieldData;

        private static final GeoDistance distFunction = GeoDistance.DEFAULT;

        public GeoFieldDataScoreFunction(GeoPoint origin, double scale, double decay, double offset, DecayFunction func,
                IndexGeoPointFieldData fieldData, MultiValueMode mode) {
            super(scale, decay, offset, func, mode);
            this.origin = origin;
            this.fieldData = fieldData;
        }

        @Override
        protected NumericDoubleValues distance(LeafReaderContext context) {
            final MultiGeoPointValues geoPointValues = fieldData.load(context).getGeoPointValues();
            return new NumericDoubleValues() {

                @Override
                public double get(int docID) {
                    geoPointValues.setDocument(docID);
                    final int num = geoPointValues.count();
                    if (num > 0) {
                        double value = mode.startDouble();
                        for (int i = 0; i < num; i++) {
                            GeoPoint other = geoPointValues.valueAt(i);
                            value = mode.apply(Math.max(0.0d, distFunction.calculate(origin.lat(), origin.lon(), other.lat(), other.lon(),
                                    DistanceUnit.METERS) - offset), value);
                        }
                        return mode.reduce(value, num);
                    } else {
                        return 0.0;
                    }
                }

            };
        }

        @Override
        protected String getDistanceString(LeafReaderContext ctx, int docId) {
            StringBuilder values = new StringBuilder(mode.name());
            values.append(" of: [");
            final MultiGeoPointValues geoPointValues = fieldData.load(ctx).getGeoPointValues();
            geoPointValues.setDocument(docId);
            final int num = geoPointValues.count();
            if (num > 0) {
                for (int i = 0; i < num; i++) {
                    GeoPoint value = geoPointValues.valueAt(i);
                    values.append("Math.max(arcDistance(");
                    values.append(value).append("(=doc value),").append(origin).append("(=origin)) - ").append(offset).append("(=offset), 0)");
                    if (i != num - 1) {
                        values.append(", ");
                    }
                }
            } else {
                values.append("0.0");
            }
            values.append("]");
            return values.toString();
        }

        @Override
        protected String getFieldName() {
            return fieldData.getFieldNames().fullName();
        }
    }

    static class NumericFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        private final IndexNumericFieldData fieldData;
        private final double origin;

        public NumericFieldDataScoreFunction(double origin, double scale, double decay, double offset, DecayFunction func,
                IndexNumericFieldData fieldData, MultiValueMode mode) {
            super(scale, decay, offset, func, mode);
            this.fieldData = fieldData;
            this.origin = origin;
        }

        @Override
        protected NumericDoubleValues distance(LeafReaderContext context) {
            final SortedNumericDoubleValues doubleValues = fieldData.load(context).getDoubleValues();
            return new NumericDoubleValues() {
                @Override
                public double get(int docID) {
                    doubleValues.setDocument(docID);
                    final int num = doubleValues.count();
                    if (num > 0) {
                        double value = mode.startDouble();
                        for (int i = 0; i < num; i++) {
                            final double other = doubleValues.valueAt(i);
                            value = mode.apply(Math.max(0.0d, Math.abs(other - origin) - offset), value);
                        }
                        return mode.reduce(value, num);
                    } else {
                        return 0.0;
                    }
                }
            };
        }

        @Override
        protected String getDistanceString(LeafReaderContext ctx, int docId) {

            StringBuilder values = new StringBuilder(mode.name());
            values.append("[");
            final SortedNumericDoubleValues doubleValues = fieldData.load(ctx).getDoubleValues();
            doubleValues.setDocument(docId);
            final int num = doubleValues.count();
            if (num > 0) {
                for (int i = 0; i < num; i++) {
                    double value = doubleValues.valueAt(i);
                    values.append("Math.max(Math.abs(");
                    values.append(value).append("(=doc value) - ").append(origin).append("(=origin))) - ").append(offset).append("(=offset), 0)");
                    if (i != num - 1) {
                        values.append(", ");
                    }
                }
            } else {
                values.append("0.0");
            }
            values.append("]");
            return values.toString();

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
        protected final MultiValueMode mode;

        public AbstractDistanceScoreFunction(double userSuppiedScale, double decay, double offset, DecayFunction func, MultiValueMode mode) {
            super(CombineFunction.MULT);
            this.mode = mode;
            if (userSuppiedScale <= 0.0) {
                throw new ElasticsearchIllegalArgumentException(FunctionScoreQueryParser.NAME + " : scale must be > 0.0.");
            }
            if (decay <= 0.0 || decay >= 1.0) {
                throw new ElasticsearchIllegalArgumentException(FunctionScoreQueryParser.NAME
                        + " : decay must be in the range [0..1].");
            }
            this.scale = func.processScale(userSuppiedScale, decay);
            this.func = func;
            if (offset < 0.0d) {
                throw new ElasticsearchIllegalArgumentException(FunctionScoreQueryParser.NAME + " : offset must be > 0.0");
            }
            this.offset = offset;
        }

        /**
         * This function computes the distance from a defined origin. Since
         * the value of the document is read from the index, it cannot be
         * guaranteed that the value actually exists. If it does not, we assume
         * the user handles this case in the query and return 0.
         * */
        protected abstract NumericDoubleValues distance(LeafReaderContext context);

        @Override
        public final LeafScoreFunction getLeafScoreFunction(final LeafReaderContext ctx) {
            final NumericDoubleValues distance = distance(ctx);
            return new LeafScoreFunction() {

                @Override
                public double score(int docId, float subQueryScore) {
                    return func.evaluate(distance.get(docId), scale);
                }

                @Override
                public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                    return Explanation.match(
                            CombineFunction.toFloat(score(docId, subQueryScore.getValue())),
                            "Function for field " + getFieldName() + ":",
                            func.explainFunction(getDistanceString(ctx, docId), distance.get(docId), scale));
                }
            };
        }

        protected abstract String getDistanceString(LeafReaderContext ctx, int docId);

        protected abstract String getFieldName();
    }

}
