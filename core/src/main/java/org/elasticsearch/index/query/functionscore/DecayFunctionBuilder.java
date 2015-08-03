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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryValidationException;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public abstract class DecayFunctionBuilder extends ScoreFunctionBuilder<DecayFunctionBuilder> {

    protected static final String ORIGIN = "origin";
    protected static final String SCALE = "scale";
    protected static final String DECAY = "decay";
    protected static final String OFFSET = "offset";

    public static final double DEFAULT_DECAY = 0.5;
    public static final double DEFAULT_OFFSET_NUMBER = 0.0d;
    public static final String DEFAULT_OFFSET_GEO = "0km";
    public static final String DEFAULT_OFFSET_DATE = "0d";
    public static final MultiValueMode DEFAULT_MULTI_VALUE_MODE = MultiValueMode.MIN;

    private String fieldName;
    private Object origin;
    private Object scale;
    private double decay = DEFAULT_DECAY;
    private Object offset;  // field type specific default not known in advance
    private MultiValueMode multiValueMode = DEFAULT_MULTI_VALUE_MODE;

    /**
     * Override this function if you want to produce your own scorer.
     */
    public abstract DecayFunction getDecayFunction();

    public DecayFunctionBuilder(String fieldName, Object origin, Object scale) {
        this.fieldName = fieldName;
        this.origin = origin;
        this.scale = scale;
    }

    public DecayFunctionBuilder setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public DecayFunctionBuilder setOrigin(Object origin) {
        this.origin = origin;
        return this;
    }

    public DecayFunctionBuilder setScale(Object scale) {
        this.scale = scale;
        return this;
    }

    public DecayFunctionBuilder setDecay(double decay) {
        this.decay = decay;
        return this;
    }

    public DecayFunctionBuilder setOffset(Object offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getWriteableName());
        builder.startObject(fieldName);
        if (origin != null) {
            builder.field(ORIGIN, origin);
        }
        builder.field(SCALE, scale);
        builder.field(DECAY, decay);
        if (offset != null) {
            builder.field(OFFSET, offset);
        }
        builder.endObject();
        builder.field(DecayFunctionParser.MULTI_VALUE_MODE.getPreferredName(), multiValueMode.name());
        builder.endObject();
    }

    public ScoreFunctionBuilder setMultiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
        return this;
    }

    public ScoreFunctionBuilder setMultiValueMode(String multiValueMode) {
        this.multiValueMode = MultiValueMode.fromString(multiValueMode.toUpperCase(Locale.ROOT));
        return this;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (fieldName == null) {
            validationException = addValidationError("field name cannot be null", validationException);
        }
        if (scale == null) {
            validationException = addValidationError("scale must not be null", validationException);
        }
        if (decay <= 0 || decay >= 1.0) {
            validationException = addValidationError("decay must be in range 0..1!", validationException);
        }
        return validationException;
    }

    @Override
    protected ScoreFunction doScoreFunction(QueryShardContext parseContext) throws IOException {
        // now, the field must exist, else we cannot read the value for the doc later
        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new IllegalArgumentException("unknown field ["+fieldName+"]");
        }
        // dates and time need special handling
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            return handleDateVariable(parseContext, (DateFieldMapper.DateFieldType) fieldType);
        } else if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType) {
            return handleGeoVariable(parseContext, (GeoPointFieldMapper.GeoPointFieldType) fieldType);
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return handleNumberVariable(parseContext, (NumberFieldMapper.NumberFieldType) fieldType);
        } else {
            throw new IllegalArgumentException("field ["+fieldName+"] is of type ["+fieldType+"], but only numeric types are supported.");
        }
    }

    private AbstractDistanceScoreFunction handleNumberVariable(
            QueryShardContext parseContext, NumberFieldMapper.NumberFieldType fieldType) throws IOException {
        // to be consistent with the parser
        if (this.scale == null || this.origin == null) {
            throw new IllegalArgumentException("both ["+DecayFunctionBuilder.SCALE+"] and ["+DecayFunctionBuilder.ORIGIN+"] " +
                    "must be set for numeric fields.");
        }
        // handle field specific defaults
        double origin = getAsDouble(this.origin, 0, "[{" + DecayFunctionBuilder.ORIGIN + "}] must be a double for numeric fields.");
        double scale = getAsDouble(this.scale, 0, "[{" + DecayFunctionBuilder.SCALE + "}] must be a double for numeric fields.");
        double offset = getAsDouble(this.offset, DEFAULT_OFFSET_NUMBER, "[{" + DecayFunctionBuilder.OFFSET + "}] must be a double for numeric fields.");;
        // create the function
        //NO COMMIT indexService needs to available in our parseContext test!
        IndexNumericFieldData numericFieldData = parseContext.getForField(fieldType);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData, multiValueMode);
    }

    private AbstractDistanceScoreFunction handleGeoVariable(
            QueryShardContext parseContext, GeoPointFieldMapper.GeoPointFieldType fieldType) throws IOException {
        // to be consistent with the parser
        if (this.scale == null || this.origin == null) {
            throw new IllegalArgumentException("both ["+DecayFunctionBuilder.SCALE+"] and ["+DecayFunctionBuilder.ORIGIN+"] " +
                    "must be set for geo fields.");
        }
        // handle field specific defaults
        GeoPoint origin = getAsGeoPoint(this.origin, new GeoPoint(), "[{"+DecayFunctionBuilder.ORIGIN+"}] must be a geo point for geo fields.");
        String scaleString = getAsString(scale, null, "[{" + DecayFunctionBuilder.SCALE + "}] must be a string for geo fields.");
        String offsetString = getAsString(offset, DEFAULT_OFFSET_GEO, "[{" + DecayFunctionBuilder.OFFSET + "}] must be a string for geo fields.");
        // create the function
        double scale = DistanceUnit.DEFAULT.parse(scaleString, DistanceUnit.DEFAULT);
        double offset = DistanceUnit.DEFAULT.parse(offsetString, DistanceUnit.DEFAULT);
        //NO COMMIT indexService needs to available in our parseContext test!
        IndexGeoPointFieldData indexFieldData = parseContext.getForField(fieldType);
        return new GeoFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), indexFieldData, multiValueMode);
    }

    private AbstractDistanceScoreFunction handleDateVariable(
            QueryShardContext parseContext, DateFieldMapper.DateFieldType dateFieldType) throws IOException {
        // to be consistent with the parser
        if (this.scale == null) {
            throw new IllegalArgumentException("["+DecayFunctionBuilder.SCALE+"] must be set for date fields.");
        }
        // handle field specific defaults
        String originString = getAsString(origin, null, "[{"+DecayFunctionBuilder.ORIGIN+"}] must be a string for date fields.");
        String scaleString = getAsString(scale, null, "[{"+DecayFunctionBuilder.SCALE+"}] must be a string for date fields.");
        String offsetString = getAsString(offset, DEFAULT_OFFSET_DATE, "[{"+DecayFunctionBuilder.OFFSET+"}] must be a string for date fields.");
        // create the function
        long origin = parseContext.nowInMillis();
        if (originString != null) {
            origin = dateFieldType.parseToMilliseconds(originString, false, null, null);
        }
        TimeValue val = TimeValue.parseTimeValue(scaleString, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale");
        double scale = val.getMillis();
        val = TimeValue.parseTimeValue(offsetString, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset");
        double offset = val.getMillis();
        //NO COMMIT indexService needs to available in our parseContext test!
        IndexNumericFieldData numericFieldData = parseContext.getForField(dateFieldType);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData, multiValueMode);
    }

    private static String getAsString(Object obj, @Nullable String defaultValue, String err) {
        if (obj == null) {
            return defaultValue;
        } else if (obj instanceof String) {
            return (String) obj;
        } else {
            throw new IllegalArgumentException(err);
        }
    }

    private static GeoPoint getAsGeoPoint(Object obj, @Nullable GeoPoint defaultValue, String err) {
        if (obj == null) {
            return defaultValue;
        } else if (obj instanceof GeoPoint) {
            return (GeoPoint) obj;
        } else {
            throw new IllegalArgumentException(err);
        }
    }

    private static double getAsDouble(Object obj, @Nullable double defaultValue, String err) {
        if (obj == null) {
            return defaultValue;
        } else if (obj instanceof Double) {
            return (Double) obj;
        } else {
            throw new IllegalArgumentException(err);
        }
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
        public boolean needsScores() {
            return false;
        }

        @Override
        protected NumericDoubleValues distance(LeafReaderContext context) {
            final MultiGeoPointValues geoPointValues = fieldData.load(context).getGeoPointValues();
            return mode.select(new MultiValueMode.UnsortedNumericDoubleValues() {
                @Override
                public int count() {
                    return geoPointValues.count();
                }

                @Override
                public void setDocument(int docId) {
                    geoPointValues.setDocument(docId);
                }

                @Override
                public double valueAt(int index) {
                    GeoPoint other = geoPointValues.valueAt(index);
                    return Math.max(0.0d, distFunction.calculate(origin.lat(), origin.lon(), other.lat(), other.lon(), DistanceUnit.METERS) - offset);
                }
            }, 0.0);
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
        public boolean needsScores() {
            return false;
        }

        @Override
        protected NumericDoubleValues distance(LeafReaderContext context) {
            final SortedNumericDoubleValues doubleValues = fieldData.load(context).getDoubleValues();
            return mode.select(new MultiValueMode.UnsortedNumericDoubleValues() {
                @Override
                public int count() {
                    return doubleValues.count();
                }

                @Override
                public void setDocument(int docId) {
                    doubleValues.setDocument(docId);
                }

                @Override
                public double valueAt(int index) {
                    return Math.max(0.0d, Math.abs(doubleValues.valueAt(index) - origin) - offset);
                }
            }, 0.0);
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
                throw new IllegalArgumentException(FunctionScoreQueryBuilder.NAME + " : scale must be > 0.0.");
            }
            if (decay <= 0.0 || decay >= 1.0) {
                throw new IllegalArgumentException(FunctionScoreQueryBuilder.NAME
                        + " : decay must be in the range [0..1].");
            }
            this.scale = func.processScale(userSuppiedScale, decay);
            this.func = func;
            if (offset < 0.0d) {
                throw new IllegalArgumentException(FunctionScoreQueryBuilder.NAME + " : offset must be > 0.0");
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

    /**
     * @return an empty {@link ScoreFunctionBuilder} instance for this builder that can be used for deserialization
     */
    //NO COMMIT this may be quite convoluted ..
    protected abstract DecayFunctionBuilder getBuilderPrototype();

    @Override
    protected DecayFunctionBuilder doReadFrom(StreamInput in) throws IOException {
        DecayFunctionBuilder decayFunctionBuilder = getBuilderPrototype();
        decayFunctionBuilder.fieldName = in.readString();
        decayFunctionBuilder.origin = in.readGenericValue();
        decayFunctionBuilder.scale = in.readGenericValue();
        decayFunctionBuilder.decay = in.readDouble();
        decayFunctionBuilder.offset = in.readGenericValue();
        decayFunctionBuilder.multiValueMode = MultiValueMode.fromString(in.readString());
        return decayFunctionBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(origin);
        out.writeGenericValue(scale);
        out.writeDouble(decay);
        out.writeGenericValue(offset);
        out.writeString(multiValueMode.name());
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, origin, scale, decay, offset, multiValueMode);
    }

    @Override
    protected boolean doEquals(DecayFunctionBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(origin, other.origin) &&
                Objects.equals(scale, other.scale) &&
                Objects.equals(decay, other.decay) &&
                Objects.equals(offset, other.offset) &&
                Objects.equals(multiValueMode, other.multiValueMode);
    }
}
