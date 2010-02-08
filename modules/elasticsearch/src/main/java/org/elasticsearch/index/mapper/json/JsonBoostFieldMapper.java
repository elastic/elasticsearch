/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.json;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.analysis.NumericFloatAnalyzer;
import org.elasticsearch.index.mapper.BoostFieldMapper;
import org.elasticsearch.util.Numbers;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonBoostFieldMapper extends JsonNumberFieldMapper<Float> implements BoostFieldMapper {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final String NAME = "_boost";
        public static final Float NULL_VALUE = null;
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.NO;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonBoostFieldMapper> {

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

        @Override public JsonBoostFieldMapper build(BuilderContext context) {
            return new JsonBoostFieldMapper(name, buildIndexName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }


    private final Float nullValue;

    protected JsonBoostFieldMapper() {
        this(Defaults.NAME, Defaults.NAME);
    }

    protected JsonBoostFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.PRECISION_STEP, Defaults.INDEX, Defaults.STORE,
                Defaults.BOOST, Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Defaults.NULL_VALUE);
    }

    protected JsonBoostFieldMapper(String name, String indexName, int precisionStep, Field.Index index, Field.Store store,
                                   float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                   Float nullValue) {
        super(name, indexName, name, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NumericFloatAnalyzer(precisionStep), new NumericFloatAnalyzer(Integer.MAX_VALUE));
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 32;
    }

    @Override public Float value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return Float.NaN;
        }
        return Numbers.bytesToFloat(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Float.parseFloat(value));
    }

    @Override public String indexedValue(Float value) {
        return NumericUtils.floatToPrefixCoded(value);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newFloatRange(indexName, precisionStep,
                lowerTerm == null ? null : Float.parseFloat(lowerTerm),
                upperTerm == null ? null : Float.parseFloat(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newFloatRange(indexName, precisionStep,
                lowerTerm == null ? null : Float.parseFloat(lowerTerm),
                upperTerm == null ? null : Float.parseFloat(upperTerm),
                includeLower, includeUpper);
    }

    @Override public void parse(JsonParseContext jsonContext) throws IOException {
        // we override parse since we want to handle cases where it is not indexed and not stored (the default)
        float value = parsedFloatValue(jsonContext);
        if (!Float.isNaN(value)) {
            jsonContext.doc().setBoost(value);
        }
        super.parse(jsonContext);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        float value = parsedFloatValue(jsonContext);
        if (Float.isNaN(value)) {
            return null;
        }
        jsonContext.doc().setBoost(value);
        Field field = null;
        if (stored()) {
            field = new Field(indexName, Numbers.floatToBytes(value), store);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setFloatValue(value));
            }
        } else if (indexed()) {
            field = new Field(indexName, popCachedStream(precisionStep).setFloatValue(value));
        }
        return field;
    }

    private float parsedFloatValue(JsonParseContext jsonContext) throws IOException {
        float value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return Float.NaN;
            }
            value = nullValue;
        } else {
            value = jsonContext.jp().getFloatValue();
        }
        return value;
    }

    @Override public int sortType() {
        return SortField.FLOAT;
    }
}
