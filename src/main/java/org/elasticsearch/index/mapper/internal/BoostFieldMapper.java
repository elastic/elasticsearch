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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericFloatAnalyzer;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class BoostFieldMapper extends NumberFieldMapper<Float> implements InternalMapper, RootMapper {

    public static final String CONTENT_TYPE = "_boost";
    public static final String NAME = "_boost";

    public static class Defaults extends NumberFieldMapper.Defaults {
        public static final String NAME = "_boost";
        public static final Float NULL_VALUE = null;
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.NO;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, BoostFieldMapper> {

        protected Float nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
            index = Defaults.INDEX;
            store = Defaults.STORE;
        }

        public Builder nullValue(float nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public BoostFieldMapper build(BuilderContext context) {
            return new BoostFieldMapper(name, buildIndexName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String fieldName, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            String name = node.get("name") == null ? BoostFieldMapper.Defaults.NAME : node.get("name").toString();
            BoostFieldMapper.Builder builder = MapperBuilders.boost(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(nodeFloatValue(propNode));
                }
            }
            return builder;
        }
    }

    private final Float nullValue;

    public BoostFieldMapper() {
        this(Defaults.NAME, Defaults.NAME);
    }

    protected BoostFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.PRECISION_STEP, Defaults.INDEX, Defaults.STORE,
                Defaults.BOOST, Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Defaults.NULL_VALUE);
    }

    protected BoostFieldMapper(String name, String indexName, int precisionStep, Field.Index index, Field.Store store,
                               float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                               Float nullValue) {
        super(new Names(name, indexName, indexName, name), precisionStep, null, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NamedAnalyzer("_float/" + precisionStep, new NumericFloatAnalyzer(precisionStep)),
                new NamedAnalyzer("_float/max", new NumericFloatAnalyzer(Integer.MAX_VALUE)));
        this.nullValue = nullValue;
    }

    @Override
    protected int maxPrecisionStep() {
        return 32;
    }

    @Override
    public Float value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return null;
        }
        return Numbers.bytesToFloat(value);
    }

    @Override
    public Float valueFromString(String value) {
        return Float.parseFloat(value);
    }

    @Override
    public String indexedValue(String value) {
        return NumericUtils.floatToPrefixCoded(Float.parseFloat(value));
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions) {
        float iValue = Float.parseFloat(value);
        float iSim = Float.parseFloat(minSim);
        return NumericRangeQuery.newFloatRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query fuzzyQuery(String value, double minSim, int prefixLength, int maxExpansions) {
        float iValue = Float.parseFloat(value);
        float iSim = (float) (minSim * dFuzzyFactor);
        return NumericRangeQuery.newFloatRange(names.indexName(), precisionStep,
                iValue - iSim,
                iValue + iSim,
                true, true);
    }

    @Override
    public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeQuery.newFloatRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : Float.parseFloat(lowerTerm),
                upperTerm == null ? null : Float.parseFloat(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFilter.newFloatRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : Float.parseFloat(lowerTerm),
                upperTerm == null ? null : Float.parseFloat(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(FieldDataCache fieldDataCache, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return NumericRangeFieldDataFilter.newFloatRange(fieldDataCache, names.indexName(),
                lowerTerm == null ? null : Float.parseFloat(lowerTerm),
                upperTerm == null ? null : Float.parseFloat(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // we override parse since we want to handle cases where it is not indexed and not stored (the default)
        float value = parseFloatValue(context);
        if (!Float.isNaN(value)) {
            context.doc().setBoost(value);
        }
        super.parse(context);
    }

    @Override
    protected Fieldable parseCreateField(ParseContext context) throws IOException {
        final float value = parseFloatValue(context);
        if (Float.isNaN(value)) {
            return null;
        }
        context.doc().setBoost(value);
        return new FloatFieldMapper.CustomFloatNumericField(this, value);
    }

    private float parseFloatValue(ParseContext context) throws IOException {
        float value;
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            if (nullValue == null) {
                return Float.NaN;
            }
            value = nullValue;
        } else {
            value = context.parser().floatValue();
        }
        return value;
    }

    @Override
    public FieldDataType fieldDataType() {
        return FieldDataType.DefaultTypes.FLOAT;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // all are defaults, don't write it at all
        if (name().equals(Defaults.NAME) && nullValue == null) {
            return builder;
        }
        builder.startObject(contentType());
        if (!name().equals(Defaults.NAME)) {
            builder.field("name", name());
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
