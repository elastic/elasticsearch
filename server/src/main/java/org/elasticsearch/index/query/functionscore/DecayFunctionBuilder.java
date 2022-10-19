/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class DecayFunctionBuilder<DFB extends DecayFunctionBuilder<DFB>> extends ScoreFunctionBuilder<DFB> {

    protected static final String ORIGIN = "origin";
    protected static final String SCALE = "scale";
    protected static final String DECAY = "decay";
    protected static final String OFFSET = "offset";

    public static final double DEFAULT_DECAY = 0.5;
    public static final MultiValueMode DEFAULT_MULTI_VALUE_MODE = MultiValueMode.MIN;

    private final String fieldName;
    // parsing of origin, scale, offset and decay depends on the field type, delayed to the data node that has the mapping for it
    private final BytesReference functionBytes;
    private MultiValueMode multiValueMode = DEFAULT_MULTI_VALUE_MODE;

    /**
     * Convenience constructor that converts its parameters into json to parse on the data nodes.
     */
    protected DecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        this(fieldName, origin, scale, offset, DEFAULT_DECAY);
    }

    /**
     * Convenience constructor that converts its parameters into json to parse on the data nodes.
     */
    protected DecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        if (fieldName == null) {
            throw new IllegalArgumentException("decay function: field name must not be null");
        }
        if (scale == null) {
            throw new IllegalArgumentException("decay function: scale must not be null");
        }
        if (decay <= 0 || decay >= 1.0) {
            throw new IllegalStateException("decay function: decay must be in range 0..1!");
        }
        this.fieldName = fieldName;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            if (origin != null) {
                builder.field(ORIGIN, origin);
            }
            builder.field(SCALE, scale);
            if (offset != null) {
                builder.field(OFFSET, offset);
            }
            builder.field(DECAY, decay);
            builder.endObject();
            this.functionBytes = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to build inner function object", e);
        }
    }

    protected DecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        if (fieldName == null) {
            throw new IllegalArgumentException("decay function: field name must not be null");
        }
        if (functionBytes == null) {
            throw new IllegalArgumentException("decay function: function must not be null");
        }
        this.fieldName = fieldName;
        this.functionBytes = functionBytes;
    }

    /**
     * Read from a stream.
     */
    protected DecayFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        functionBytes = in.readBytesReference();
        multiValueMode = MultiValueMode.readMultiValueModeFrom(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeBytesReference(functionBytes);
        multiValueMode.writeTo(out);
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public BytesReference getFunctionBytes() {
        return this.functionBytes;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        try (InputStream stream = functionBytes.streamInput()) {
            builder.rawField(fieldName, stream);
        }
        builder.field(DecayFunctionParser.MULTI_VALUE_MODE.getPreferredName(), multiValueMode.name());
        builder.endObject();
    }

    @SuppressWarnings("unchecked")
    public DFB setMultiValueMode(MultiValueMode multiValueMode) {
        if (multiValueMode == null) {
            throw new IllegalArgumentException("decay function: multi_value_mode must not be null");
        }
        this.multiValueMode = multiValueMode;
        return (DFB) this;
    }

    public MultiValueMode getMultiValueMode() {
        return this.multiValueMode;
    }

    @Override
    protected boolean doEquals(DFB functionBuilder) {
        return Objects.equals(this.fieldName, functionBuilder.getFieldName())
            && Objects.equals(this.functionBytes, functionBuilder.getFunctionBytes())
            && Objects.equals(this.multiValueMode, functionBuilder.getMultiValueMode());
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.fieldName, this.functionBytes, this.multiValueMode);
    }

    @Override
    protected ScoreFunction doToFunction(SearchExecutionContext context) throws IOException {
        AbstractDistanceScoreFunction scoreFunction;
        // EMPTY is safe because parseVariable doesn't use namedObject
        try (
            InputStream stream = functionBytes.streamInput();
            XContentParser parser = XContentFactory.xContent(XContentHelper.xContentType(functionBytes))
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            scoreFunction = parseVariable(fieldName, parser, context, multiValueMode);
        }
        return scoreFunction;
    }

    /**
     * Override this function if you want to produce your own scorer.
     * */
    protected abstract DecayFunction getDecayFunction();

    private AbstractDistanceScoreFunction parseVariable(
        String fieldName,
        XContentParser parser,
        SearchExecutionContext context,
        MultiValueMode mode
    ) throws IOException {
        // the field must exist, else we cannot read the value for the doc later
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new ParsingException(parser.getTokenLocation(), "unknown field [{}]", fieldName);
        }

        // dates and time and geo need special handling
        parser.nextToken();
        // TODO these ain't gonna work with runtime fields
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            return parseDateVariable(parser, context, fieldType, mode);
        } else if (fieldType instanceof GeoPointFieldType) {
            return parseGeoVariable(parser, context, fieldType, mode);
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return parseNumberVariable(parser, context, fieldType, mode);
        } else {
            throw new ParsingException(
                parser.getTokenLocation(),
                "field [{}] is of type [{}], but only numeric types are supported.",
                fieldName,
                fieldType
            );
        }
    }

    private AbstractDistanceScoreFunction parseNumberVariable(
        XContentParser parser,
        SearchExecutionContext context,
        MappedFieldType fieldType,
        MultiValueMode mode
    ) throws IOException {
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
            } else if (DecayFunctionBuilder.SCALE.equals(parameterName)) {
                scale = parser.doubleValue();
                scaleFound = true;
            } else if (DecayFunctionBuilder.DECAY.equals(parameterName)) {
                decay = parser.doubleValue();
            } else if (DecayFunctionBuilder.ORIGIN.equals(parameterName)) {
                origin = parser.doubleValue();
                refFound = true;
            } else if (DecayFunctionBuilder.OFFSET.equals(parameterName)) {
                offset = parser.doubleValue();
            } else {
                throw new ElasticsearchParseException("parameter [{}] not supported!", parameterName);
            }
        }
        if (scaleFound == false || refFound == false) {
            throw new ElasticsearchParseException(
                "both [{}] and [{}] must be set for numeric fields.",
                DecayFunctionBuilder.SCALE,
                DecayFunctionBuilder.ORIGIN
            );
        }
        IndexNumericFieldData numericFieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData, mode);
    }

    private AbstractDistanceScoreFunction parseGeoVariable(
        XContentParser parser,
        SearchExecutionContext context,
        MappedFieldType fieldType,
        MultiValueMode mode
    ) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        GeoPoint origin = new GeoPoint();
        String scaleString = null;
        String offsetString = "0km";
        double decay = 0.5;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (DecayFunctionBuilder.SCALE.equals(parameterName)) {
                scaleString = parser.text();
            } else if (DecayFunctionBuilder.ORIGIN.equals(parameterName)) {
                origin = GeoUtils.parseGeoPoint(parser);
            } else if (DecayFunctionBuilder.DECAY.equals(parameterName)) {
                decay = parser.doubleValue();
            } else if (DecayFunctionBuilder.OFFSET.equals(parameterName)) {
                offsetString = parser.text();
            } else {
                throw new ElasticsearchParseException("parameter [{}] not supported!", parameterName);
            }
        }
        if (origin == null || scaleString == null) {
            throw new ElasticsearchParseException(
                "[{}] and [{}] must be set for geo fields.",
                DecayFunctionBuilder.ORIGIN,
                DecayFunctionBuilder.SCALE
            );
        }
        double scale = DistanceUnit.DEFAULT.parse(scaleString, DistanceUnit.DEFAULT);
        double offset = DistanceUnit.DEFAULT.parse(offsetString, DistanceUnit.DEFAULT);
        IndexGeoPointFieldData indexFieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
        return new GeoFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), indexFieldData, mode);

    }

    private AbstractDistanceScoreFunction parseDateVariable(
        XContentParser parser,
        SearchExecutionContext context,
        MappedFieldType dateFieldType,
        MultiValueMode mode
    ) throws IOException {
        XContentParser.Token token;
        String parameterName = null;
        String scaleString = null;
        String originString = null;
        String offsetString = "0d";
        double decay = 0.5;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                parameterName = parser.currentName();
            } else if (DecayFunctionBuilder.SCALE.equals(parameterName)) {
                scaleString = parser.text();
            } else if (DecayFunctionBuilder.ORIGIN.equals(parameterName)) {
                originString = parser.text();
            } else if (DecayFunctionBuilder.DECAY.equals(parameterName)) {
                decay = parser.doubleValue();
            } else if (DecayFunctionBuilder.OFFSET.equals(parameterName)) {
                offsetString = parser.text();
            } else {
                throw new ElasticsearchParseException("parameter [{}] not supported!", parameterName);
            }
        }
        long origin;
        if (originString == null) {
            origin = context.nowInMillis();
        } else {
            origin = ((DateFieldMapper.DateFieldType) dateFieldType).parseToLong(originString, false, null, null, context::nowInMillis);
        }

        if (scaleString == null) {
            throw new ElasticsearchParseException("[{}] must be set for date fields.", DecayFunctionBuilder.SCALE);
        }
        TimeValue val = TimeValue.parseTimeValue(
            scaleString,
            TimeValue.timeValueHours(24),
            DecayFunctionParser.class.getSimpleName() + ".scale"
        );
        double scale = val.getMillis();
        val = TimeValue.parseTimeValue(offsetString, TimeValue.timeValueHours(24), DecayFunctionParser.class.getSimpleName() + ".offset");
        double offset = val.getMillis();
        IndexNumericFieldData numericFieldData = context.getForField(dateFieldType, MappedFieldType.FielddataOperation.SEARCH);
        return new NumericFieldDataScoreFunction(origin, scale, decay, offset, getDecayFunction(), numericFieldData, mode);
    }

    static class GeoFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        private final GeoPoint origin;
        private final IndexGeoPointFieldData fieldData;

        private static final GeoDistance distFunction = GeoDistance.ARC;

        GeoFieldDataScoreFunction(
            GeoPoint origin,
            double scale,
            double decay,
            double offset,
            DecayFunction func,
            IndexGeoPointFieldData fieldData,
            MultiValueMode mode
        ) {
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
            final MultiGeoPointValues geoPointValues = fieldData.load(context).getPointValues();
            return FieldData.replaceMissing(mode.select(new SortingNumericDoubleValues() {
                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (geoPointValues.advanceExact(docId)) {
                        int n = geoPointValues.docValueCount();
                        resize(n);
                        for (int i = 0; i < n; i++) {
                            GeoPoint other = geoPointValues.nextValue();
                            double distance = distFunction.calculate(
                                origin.lat(),
                                origin.lon(),
                                other.lat(),
                                other.lon(),
                                DistanceUnit.METERS
                            );
                            values[i] = Math.max(0.0d, distance - offset);
                        }
                        sort();
                        return true;
                    } else {
                        return false;
                    }
                }
            }), 0);
        }

        @Override
        protected String getDistanceString(LeafReaderContext ctx, int docId) throws IOException {
            StringBuilder values = new StringBuilder(mode.name());
            values.append(" of: [");
            final MultiGeoPointValues geoPointValues = fieldData.load(ctx).getPointValues();
            if (geoPointValues.advanceExact(docId)) {
                final int num = geoPointValues.docValueCount();
                for (int i = 0; i < num; i++) {
                    GeoPoint value = geoPointValues.nextValue();
                    values.append("Math.max(arcDistance(");
                    values.append(value).append("(=doc value),");
                    values.append(origin).append("(=origin)) - ").append(offset).append("(=offset), 0)");
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
            return fieldData.getFieldName();
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            GeoFieldDataScoreFunction geoFieldDataScoreFunction = (GeoFieldDataScoreFunction) other;
            return super.doEquals(other) && Objects.equals(this.origin, geoFieldDataScoreFunction.origin);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(super.doHashCode(), origin);
        }
    }

    static class NumericFieldDataScoreFunction extends AbstractDistanceScoreFunction {

        private final IndexNumericFieldData fieldData;
        private final double origin;

        NumericFieldDataScoreFunction(
            double origin,
            double scale,
            double decay,
            double offset,
            DecayFunction func,
            IndexNumericFieldData fieldData,
            MultiValueMode mode
        ) {
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
            return FieldData.replaceMissing(mode.select(new SortingNumericDoubleValues() {
                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (doubleValues.advanceExact(docId)) {
                        int n = doubleValues.docValueCount();
                        resize(n);
                        for (int i = 0; i < n; i++) {
                            values[i] = Math.max(0.0d, Math.abs(doubleValues.nextValue() - origin) - offset);
                        }
                        sort();
                        return true;
                    } else {
                        return false;
                    }
                }
            }), 0);
        }

        @Override
        protected String getDistanceString(LeafReaderContext ctx, int docId) throws IOException {

            StringBuilder values = new StringBuilder(mode.name());
            values.append("[");
            final SortedNumericDoubleValues doubleValues = fieldData.load(ctx).getDoubleValues();
            if (doubleValues.advanceExact(docId)) {
                final int num = doubleValues.docValueCount();
                for (int i = 0; i < num; i++) {
                    double value = doubleValues.nextValue();
                    values.append("Math.max(Math.abs(");
                    values.append(value).append("(=doc value) - ");
                    values.append(origin).append("(=origin))) - ");
                    values.append(offset).append("(=offset), 0)");
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
            return fieldData.getFieldName();
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            NumericFieldDataScoreFunction numericFieldDataScoreFunction = (NumericFieldDataScoreFunction) other;
            if (super.doEquals(other) == false) {
                return false;
            }
            return Objects.equals(this.origin, numericFieldDataScoreFunction.origin);
        }
    }

    /**
     * This is the base class for scoring a single field.
     *
     * */
    public abstract static class AbstractDistanceScoreFunction extends ScoreFunction {

        private final double scale;
        protected final double offset;
        private final DecayFunction func;
        protected final MultiValueMode mode;

        public AbstractDistanceScoreFunction(
            double userSuppiedScale,
            double decay,
            double offset,
            DecayFunction func,
            MultiValueMode mode
        ) {
            super(CombineFunction.MULTIPLY);
            this.mode = mode;
            if (userSuppiedScale <= 0.0) {
                throw new IllegalArgumentException(FunctionScoreQueryBuilder.NAME + " : scale must be > 0.0.");
            }
            if (decay <= 0.0 || decay >= 1.0) {
                throw new IllegalArgumentException(FunctionScoreQueryBuilder.NAME + " : decay must be in the range [0..1].");
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
                public double score(int docId, float subQueryScore) throws IOException {
                    if (distance.advanceExact(docId)) {
                        return func.evaluate(distance.doubleValue(), scale);
                    } else {
                        return 0;
                    }
                }

                @Override
                public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                    if (distance.advanceExact(docId) == false) {
                        return Explanation.noMatch("No value for the distance");
                    }
                    double value = distance.doubleValue();
                    return Explanation.match(
                        (float) score(docId, subQueryScore.getValue().floatValue()),
                        "Function for field " + getFieldName() + ":",
                        func.explainFunction(getDistanceString(ctx, docId), value, scale)
                    );
                }
            };
        }

        protected abstract String getDistanceString(LeafReaderContext ctx, int docId) throws IOException;

        protected abstract String getFieldName();

        @Override
        protected boolean doEquals(ScoreFunction other) {
            AbstractDistanceScoreFunction distanceScoreFunction = (AbstractDistanceScoreFunction) other;
            return Objects.equals(this.scale, distanceScoreFunction.scale)
                && Objects.equals(this.offset, distanceScoreFunction.offset)
                && Objects.equals(this.mode, distanceScoreFunction.mode)
                && Objects.equals(this.func, distanceScoreFunction.func)
                && Objects.equals(this.getFieldName(), distanceScoreFunction.getFieldName());
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(scale, offset, mode, func, getFieldName());
        }
    }
}
