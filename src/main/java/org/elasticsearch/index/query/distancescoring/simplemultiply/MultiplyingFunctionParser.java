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

package org.elasticsearch.index.query.distancescoring.simplemultiply;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
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
import org.elasticsearch.index.query.distancescoring.ScoreFunctionParser;
import org.elasticsearch.search.internal.SearchContext;

/**
 * This class provides the basic functionality needed for adding a multiplying
 * scoring function. Suppose you have several numeric fields and you want to
 * score the documents according to some decay function from a reference point.
 * 
 * This parser accepts input of the form
 * 
 * <pre>
 * {@code}
 * { 
 *      "fieldname1" : {"reference" = "someValue", "scale" = "someValue"} 
 *      "fieldname2" : {"reference" = "someValue", "scale" = "someValue"} 
 * }
 * </pre>
 * 
 * "reference" here refers to the reference point and "scale" to the level of
 * uncertainty you have in your reference.
 * <p>
 * 
 * For example, you might want to retrieve an event that took place around the
 * 20 May 2010 somewhere near Berlin. You are mainly interested in events that
 * are close to the 20 May 2010 but you are unsure about your guess, maybe it
 * was a week before or after that. Your "reference" for the date field would be
 * "20 May 2010" and your "scale" would be "7d".
 * 
 * This class parses the input and creates a scoring function that multiplies a
 * custom scoring function for each field given.
 * <p>
 * To write a new scoring function, create a new class that inherits from this
 * one and implement the getDistanceFuntion(). Furthermore, you might need a
 * Builder to support the java API. To do so, override the getName() in
 * {@link MultiplyingFunctionBuilder}.
 * <p>
 * See {@link GaussDecayFunctionBuilder} and {@link GaussDecayFunctionParser}
 * for an example. The parser furthermore needs to be registered in the
 * {@link org.elasticsearch.index.query.distancescoring.DistanceScoringModule
 * DistanceScoringModule}. See
 * {@link org.elasticsearch.test.integration.search.distancescore.CustomDistanceScorePlugin
 * CustomDistanceScorePlugin} and
 * {@link org.elasticsearch.test.integration.search.distancescore.CustomDistanceScorePlugin
 * DistanceScorePluginTest DistanceScorePluginTest} for an example on how to
 * write your own function and plugin.
 * 
 * **/

public abstract class MultiplyingFunctionParser implements ScoreFunctionParser {

    /**
     * Override this function if you want to produce your own scorer.
     * */
    public abstract CustomDecayFuntion getDecayFunction();

    /**
     * Parses bodies of the kind
     * 
     * <pre>
     * {@code}
     * { 
     *      "fieldname1" : {"reference" = "someValue","scale" = "someValue"},
     *      "fieldname1" : {"reference" = "someValue","scale" = "someValue"} 
     * }
     * </pre>
     * 
     * For each field, an Abstract score function is created. From these the
     * MultplyingScoreFunction is created.
     * 
     * */
    @Override
    public ScoreFunction parse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        String currentFieldName = null;
        XContentParser.Token token;
        List<ScoreFunction> vars = new ArrayList<ScoreFunction>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {

                // parse per field the reference and scale value
                ScoreFunction var = parseVariable(currentFieldName, parser, parseContext);
                vars.add(var);
            } else {
                throw new ElasticSearchParseException("must start an object here!");
            }
        }
        ScoreFunction[] functions = new ScoreFunction[vars.size()];
        return new MultplyingScoreFunction(vars.toArray(functions));
    }

    // parses a single field with reference and scale parameter
    private ScoreFunction parseVariable(String fieldName, XContentParser parser, QueryParseContext parseContext) throws IOException {

        // now, the field must exist, else we later cannot read the value for
        // the doc
        MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "failed to find field [" + fieldName + "]");
        }
        FieldMapper<?> mapper = smartMappers.mapper();

        // dates and time need special handling
        if (mapper instanceof DateFieldMapper) {
            return parseDateVariable(fieldName, parser, parseContext, (DateFieldMapper) mapper);
        } else if (mapper instanceof GeoStringFieldMapper) {
            return parseGeoVariable(fieldName, parser, parseContext, (GeoStringFieldMapper) mapper);
        } else if (mapper instanceof NumberFieldMapper<?>) {
            return parseNumberVariable(fieldName, parser, parseContext, (NumberFieldMapper<?>) mapper);
        } else {
            throw new QueryParsingException(parseContext.index(), "field of type " + mapper.fieldType() + " not supported");
        }
    }

    private ScoreFunction parseNumberVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            NumberFieldMapper<?> mapper) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        String scaleString = null;
        String referenceString = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals("scale")) {
                scaleString = parser.text();
            } else if (parameterName.equals("reference")) {
                referenceString = parser.text();
            } else {
                throw new ElasticSearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        double reference = mapper.value(referenceString).doubleValue();
        double scale = mapper.value(scaleString).doubleValue();
        IndexNumericFieldData<?> numericFieldData = parseContext.fieldData().getForField(mapper);
        return new NumericFieldDataScoreFunction(scale, getDecayFunction(), reference, numericFieldData);
    }

    private ScoreFunction parseGeoVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            GeoStringFieldMapper mapper) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        GeoPoint reference = new GeoPoint();
        String scaleString = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals("scale")) {
                scaleString = parser.text();
            } else if (parameterName.equals("reference")) {
                reference = GeoPoint.parse(parser);
            } else {
                throw new ElasticSearchParseException("Parameter " + parameterName + " not supported!");
            }
        }

        if (scaleString == null) {
            scaleString = "1km";
        }
        if (reference == null) {
            throw new ElasticSearchParseException("Need a geo point that gives the loacation!");
        }
        double scale = DistanceUnit.parse(scaleString, DistanceUnit.METERS, DistanceUnit.METERS);

        IndexGeoPointFieldData<?> indexFieldData = parseContext.fieldData().getForField(mapper);
        return new GeoFieldDataScoreFunction(scale, getDecayFunction(), reference, indexFieldData);

    }

    private ScoreFunction parseDateVariable(String fieldName, XContentParser parser, QueryParseContext parseContext,
            DateFieldMapper dateFieldMapper) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        String scaleString = null;
        String referenceString = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (parameterName.equals("scale")) {
                scaleString = parser.text();
            } else if (parameterName.equals("reference")) {
                referenceString = parser.text();
            } else {
                throw new ElasticSearchParseException("Parameter " + parameterName + " not supported!");
            }
        }
        long reference = SearchContext.current().nowInMillis();
        if (referenceString != null) {
            reference = dateFieldMapper.value(referenceString).longValue();
        }
        double scale = 1.e7;
        if (scaleString != null) {
            TimeValue val = TimeValue.parseTimeValue(scaleString, TimeValue.timeValueHours(24));
            scale = val.getMillis();
        }
        IndexNumericFieldData<?> numericFieldData = parseContext.fieldData().getForField(dateFieldMapper);
        return new NumericFieldDataScoreFunction(scale, getDecayFunction(), reference, numericFieldData);
    }

    /**
     * This class does the actual scoring. It gathers scores for each field that
     * the user provided and multiplies them.
     * 
     * */
    public static class MultplyingScoreFunction implements ScoreFunction {

        private final ScoreFunction[] functions;

        public MultplyingScoreFunction(ScoreFunction[] functions) {
            this.functions = functions;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            for (ScoreFunction scoreFunction : functions) {
                scoreFunction.setNextReader(context);
            }
        }

        @Override
        public float score(int docId, float subQueryScore) {
            return subQueryScore * factor(docId);
        }

        @Override
        public float factor(int docId) {
            float multScore = 1.0f;
            for (ScoreFunction scoreFunction : functions) {
                multScore *= scoreFunction.factor(docId);
            }
            return multScore;
        }

        @Override
        public Explanation explainScore(int docId, Explanation subQueryExpl) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setMatch(true);
            ce.setValue(factor(docId) * subQueryExpl.getValue());
            ce.setDescription(subQueryExpl.getValue() + "*Product of field functions for doc " + docId + ":");
            for (ScoreFunction sf : functions) {
                ce.addDetail(sf.explainFactor(docId));
            }
            return ce;
        }

        @Override
        public Explanation explainFactor(int docId) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setMatch(true);
            ce.setValue(factor(docId));
            ce.setDescription("Product of field functions for doc " + docId + ":");
            for (ScoreFunction sf : functions) {
                ce.addDetail(sf.explainFactor(docId));
            }
            return ce;
        }
    }

    static class GeoFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        GeoPoint reference = null;
        IndexGeoPointFieldData<?> fieldData = null;
        GeoPointValues geoPointValues = null;

        private GeoDistance distFunction;

        public GeoFieldDataScoreFunction(double scale, CustomDecayFuntion func, GeoPoint reference, IndexGeoPointFieldData<?> fieldData) {
            super(scale, func);
            this.reference = reference;
            this.distFunction = GeoDistance.fromString("arc");
            this.fieldData = fieldData;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            geoPointValues = fieldData.load(context).getGeoPointValues();
        }

        @Override
        protected double distance(int docId) {
            if (geoPointValues.hasValue(docId)) {
                final GeoPoint other = geoPointValues.getValue(docId);
                return distFunction.calculate(reference.lat(), reference.lon(), other.lat(), other.lon(), DistanceUnit.METERS);
            } else {
                return Float.MAX_VALUE;
            }
        }

        @Override
        protected String getDistanceString(int docId) {
            final GeoPoint other = geoPointValues.getValue(docId);
            return "arcDistance(" + other + "(=doc value), " + reference + ") = " + distance(docId);

        }

        @Override
        protected String getFieldName() {

            return fieldData.getFieldNames().fullName();

        }
    }

    static class NumericFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        IndexNumericFieldData<?> fieldData = null;
        private DoubleValues valueOfDoc;

        double reference = 0;

        public NumericFieldDataScoreFunction(double scale, CustomDecayFuntion func, double reference, IndexNumericFieldData<?> valueOfDoc) {
            super(scale, func);
            this.fieldData = valueOfDoc;
            this.reference = reference;
        }

        public void setNextReader(AtomicReaderContext context) {
            this.valueOfDoc = this.fieldData.load(context).getDoubleValues();
        }

        @Override
        protected double distance(int docId) {

            if (valueOfDoc.hasValue(docId)) {
                return valueOfDoc.getValue(docId) - reference;

            } else {
                return Float.MAX_VALUE;
            }

        }

        @Override
        protected String getDistanceString(int docId) {

            return "(" + valueOfDoc.getValue(docId) + "(=doc value) - " + reference + ")";
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
    public static abstract class AbstractDistanceScoreFunction implements ScoreFunction {

        private final double scale;
        private final CustomDecayFuntion func;

        public AbstractDistanceScoreFunction(double scale, CustomDecayFuntion func) {
            this.scale = scale;
            this.func = func;
        }

        @Override
        public float score(int docId, float subQueryScore) {
            return subQueryScore * factor(docId);
        }

        @Override
        public float factor(int docId) {
            double value = distance(docId);
            return (float) func.evaluate(value, scale);
        }

        /**
         * This function computes the distance from a defined reference. Since
         * the value of the document is read from the index, it cannot be
         * guaranteed that the value actually exists. If it does not, we assume
         * the user is not interested in the document since it cannot be scored
         * and therefore this function should return Float.MAX_VALUE in this
         * case.
         * */
        protected abstract double distance(int docId);

        protected abstract String getDistanceString(int docId);

        protected abstract String getFieldName();

        @Override
        public Explanation explainScore(int docId, Explanation subQueryExpl) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setValue(subQueryExpl.getValue() * factor(docId));
            ce.setMatch(true);
            ce.setDescription("subQueryScore*Function for field " + getFieldName() + ":");
            ce.addDetail(func.explainFunction(getDistanceString(docId), distance(docId), scale));
            return ce;
        }

        @Override
        public Explanation explainFactor(int docId) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setValue(factor(docId));
            ce.setMatch(true);
            ce.setDescription("Function for field " + getFieldName() + ":");
            ce.addDetail(func.explainFunction(getDistanceString(docId), distance(docId), scale));
            return ce;
        }
    }

}
