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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.hppc.DoubleArrayList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDoubleAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.util.NumericUtils.doubleToSortableLong;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class DoubleFieldMapper extends NumberFieldMapper {

    public static final String CONTENT_TYPE = "double";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final MappedFieldType FIELD_TYPE = new DoubleFieldType();

        static {
            FIELD_TYPE.freeze();
        }

        public static final Double NULL_VALUE = null;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, DoubleFieldMapper> {

        protected Double nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
            builder = this;
        }

        public Builder nullValue(double nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public DoubleFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            DoubleFieldMapper fieldMapper = new DoubleFieldMapper(fieldType, docValues, nullValue, ignoreMalformed(context), coerce(context),
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            return NumericDoubleAnalyzer.buildNamedAnalyzer(precisionStep);
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DoubleFieldMapper.Builder builder = doubleField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("nullValue") || propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeDoubleValue(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class DoubleFieldType extends NumberFieldType {

        public DoubleFieldType() {}

        protected DoubleFieldType(DoubleFieldType ref) {
            super(ref);
        }

        @Override
        public NumberFieldType clone() {
            return new DoubleFieldType(this);
        }

        @Override
        public Double value(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            if (value instanceof BytesRef) {
                return Numbers.bytesToDouble((BytesRef) value);
            }
            return Double.parseDouble(value.toString());
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            long longValue = NumericUtils.doubleToSortableLong(parseDoubleValue(value));
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(longValue, 0, bytesRef);   // 0 because of exact match
            return bytesRef.get();
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
            return NumericRangeQuery.newDoubleRange(names().indexName(), numericPrecisionStep(),
                lowerTerm == null ? null : parseDoubleValue(lowerTerm),
                upperTerm == null ? null : parseDoubleValue(upperTerm),
                includeLower, includeUpper);
        }

        @Override
        public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
            double iValue = Double.parseDouble(value);
            double iSim = fuzziness.asDouble();
            return NumericRangeQuery.newDoubleRange(names().indexName(), numericPrecisionStep(),
                iValue - iSim,
                iValue + iSim,
                true, true);
        }

        @Override
        public FieldStats stats(Terms terms, int maxDoc) throws IOException {
            double minValue = NumericUtils.sortableLongToDouble(NumericUtils.getMinLong(terms));
            double maxValue = NumericUtils.sortableLongToDouble(NumericUtils.getMaxLong(terms));
            return new FieldStats.Double(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue
            );
        }
    }

    private Double nullValue;

    private String nullValueAsString;

    protected DoubleFieldMapper(MappedFieldType fieldType, Boolean docValues, Double nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(fieldType, docValues, ignoreMalformed, coerce, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override
    public MappedFieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("double");
    }

    public Query rangeFilter(Double lowerTerm, Double upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newDoubleRange(fieldType.names().indexName(), fieldType.numericPrecisionStep(), lowerTerm, upperTerm, includeLower, includeUpper);
    }

    @Override
    public Query nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return new ConstantScoreQuery(termQuery(nullValue, null));
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        double value;
        float boost = this.fieldType.boost();
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue == null) {
                if (nullValue == null) {
                    return;
                }
                value = nullValue;
            } else if (externalValue instanceof String) {
                String sExternalValue = (String) externalValue;
                if (sExternalValue.length() == 0) {
                    if (nullValue == null) {
                        return;
                    }
                    value = nullValue;
                } else {
                    value = Double.parseDouble(sExternalValue);
                }
            } else {
                value = ((Number) externalValue).doubleValue();
            }
            if (context.includeInAll(includeInAll, this)) {
                context.allEntries().addText(fieldType.names().fullName(), Double.toString(value), boost);
            }
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL ||
                    (parser.currentToken() == XContentParser.Token.VALUE_STRING && parser.textLength() == 0)) {
                if (nullValue == null) {
                    return;
                }
                value = nullValue;
                if (nullValueAsString != null && (context.includeInAll(includeInAll, this))) {
                    context.allEntries().addText(fieldType.names().fullName(), nullValueAsString, boost);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParser.Token token;
                String currentFieldName = null;
                Double objValue = nullValue;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                objValue = parser.doubleValue(coerce.value());
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new IllegalArgumentException("unknown property [" + currentFieldName + "]");
                        }
                    }
                }
                if (objValue == null) {
                    // no value
                    return;
                }
                value = objValue;
            } else {
                value = parser.doubleValue(coerce.value());
                if (context.includeInAll(includeInAll, this)) {
                    context.allEntries().addText(fieldType.names().fullName(), parser.text(), boost);
                }
            }
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            CustomDoubleNumericField field = new CustomDoubleNumericField(this, value, (NumberFieldType)fieldType);
            field.setBoost(boost);
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            if (useSortedNumericDocValues) {
                addDocValue(context, fields, doubleToSortableLong(value));
            } else {
                CustomDoubleNumericDocValuesField field = (CustomDoubleNumericDocValuesField) context.doc().getByKey(fieldType().names().indexName());
                if (field != null) {
                    field.add(value);
                } else {
                    field = new CustomDoubleNumericDocValuesField(fieldType().names().indexName(), value);
                    context.doc().addWithKey(fieldType().names().indexName(), field);
                }
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeResult.simulate()) {
            this.nullValue = ((DoubleFieldMapper) mergeWith).nullValue;
            this.nullValueAsString = ((DoubleFieldMapper) mergeWith).nullValueAsString;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType.numericPrecisionStep() != Defaults.PRECISION_STEP_64_BIT) {
            builder.field("precision_step", fieldType.numericPrecisionStep());
        }
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        } else if (includeDefaults) {
            builder.field("include_in_all", false);
        }

    }

    public static class CustomDoubleNumericField extends CustomNumericField {

        private final double number;

        private final NumberFieldMapper mapper;

        public CustomDoubleNumericField(NumberFieldMapper mapper, double number, NumberFieldType fieldType) {
            super(mapper, number, fieldType);
            this.mapper = mapper;
            this.number = number;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) throws IOException {
            if (fieldType().indexOptions() != IndexOptions.NONE) {
                return mapper.popCachedStream().setDoubleValue(number);
            }
            return null;
        }

        @Override
        public String numericAsString() {
            return Double.toString(number);
        }
    }

    public static class CustomDoubleNumericDocValuesField extends CustomNumericDocValuesField {

        private final DoubleArrayList values;

        public CustomDoubleNumericDocValuesField(String  name, double value) {
            super(name);
            values = new DoubleArrayList();
            add(value);
        }

        public void add(double value) {
            values.add(value);
        }

        @Override
        public BytesRef binaryValue() {
            CollectionUtils.sortAndDedup(values);

            final byte[] bytes = new byte[values.size() * 8];
            for (int i = 0; i < values.size(); ++i) {
                ByteUtils.writeDoubleLE(values.get(i), bytes, i * 8);
            }
            return new BytesRef(bytes);
        }

    }
}
