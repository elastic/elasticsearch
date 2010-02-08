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
import org.elasticsearch.index.analysis.NumericDoubleAnalyzer;
import org.elasticsearch.util.Numbers;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonDoubleFieldMapper extends JsonNumberFieldMapper<Double> {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final Double NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonDoubleFieldMapper> {

        protected Double nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(double nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonDoubleFieldMapper build(BuilderContext context) {
            return new JsonDoubleFieldMapper(name, buildIndexName(context), buildFullName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    private final Double nullValue;

    protected JsonDoubleFieldMapper(String name, String indexName, String fullName, int precisionStep,
                                    Field.Index index, Field.Store store,
                                    float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                    Double nullValue) {
        super(name, indexName, fullName, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NumericDoubleAnalyzer(precisionStep), new NumericDoubleAnalyzer(Integer.MAX_VALUE));
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 64;
    }

    @Override public Double value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return Double.NaN;
        }
        return Numbers.bytesToDouble(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Double.parseDouble(value));
    }

    @Override public String indexedValue(Double value) {
        return NumericUtils.doubleToPrefixCoded(value);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newDoubleRange(indexName, precisionStep,
                lowerTerm == null ? null : Double.parseDouble(lowerTerm),
                upperTerm == null ? null : Double.parseDouble(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newDoubleRange(indexName, precisionStep,
                lowerTerm == null ? null : Double.parseDouble(lowerTerm),
                upperTerm == null ? null : Double.parseDouble(upperTerm),
                includeLower, includeUpper);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        double value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return null;
            }
            value = nullValue;
        } else {
            value = jsonContext.jp().getDoubleValue();
        }
        Field field = null;
        if (stored()) {
            field = new Field(indexName, Numbers.doubleToBytes(value), store);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setDoubleValue(value));
            }
        } else if (indexed()) {
            field = new Field(indexName, popCachedStream(precisionStep).setDoubleValue(value));
        }
        return field;
    }

    @Override public int sortType() {
        return SortField.DOUBLE;
    }
}