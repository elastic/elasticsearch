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
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;
import org.elasticsearch.util.Numbers;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonIntegerFieldMapper extends JsonNumberFieldMapper<Integer> {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final Integer NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonIntegerFieldMapper> {

        protected Integer nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(int nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonIntegerFieldMapper build(BuilderContext context) {
            return new JsonIntegerFieldMapper(name, buildIndexName(context), buildFullName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    private final Integer nullValue;

    protected JsonIntegerFieldMapper(String name, String indexName, String fullName, int precisionStep, Field.Index index, Field.Store store,
                                     float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                     Integer nullValue) {
        super(name, indexName, fullName, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NumericIntegerAnalyzer(precisionStep), new NumericIntegerAnalyzer(Integer.MAX_VALUE));
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 32;
    }

    @Override public Integer value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return Integer.MIN_VALUE;
        }
        return Numbers.bytesToInt(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Integer.parseInt(value));
    }

    @Override public String indexedValue(Integer value) {
        return NumericUtils.intToPrefixCoded(value);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newIntRange(indexName, precisionStep,
                lowerTerm == null ? null : Integer.parseInt(lowerTerm),
                upperTerm == null ? null : Integer.parseInt(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newIntRange(indexName, precisionStep,
                lowerTerm == null ? null : Integer.parseInt(lowerTerm),
                upperTerm == null ? null : Integer.parseInt(upperTerm),
                includeLower, includeUpper);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        int value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return null;
            }
            value = nullValue;
        } else {
            value = jsonContext.jp().getIntValue();
        }
        Field field = null;
        if (stored()) {
            field = new Field(indexName, Numbers.intToBytes(value), store);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setIntValue(value));
            }
        } else if (indexed()) {
            field = new Field(indexName, popCachedStream(precisionStep).setIntValue(value));
        }
        return field;
    }

    @Override public int sortType() {
        return SortField.INT;
    }
}
