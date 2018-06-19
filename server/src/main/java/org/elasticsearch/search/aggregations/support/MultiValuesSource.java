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
package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to encapsulate a set of ValuesSource objects labeled by field name
 */
public abstract class MultiValuesSource <VS extends ValuesSource> {

    public static class Wrapper<VS> {
        private MultiValueMode multiValueMode;
        private VS valueSource;

        public Wrapper(MultiValueMode multiValueMode, VS value) {
            this.multiValueMode = multiValueMode;
            this.valueSource = value;
        }

        public MultiValueMode getMultiValueMode() {
            return multiValueMode;
        }

        public VS getValueSource() {
            return valueSource;
        }
    }

    protected Map<String, Wrapper<VS>> values;

    public static class NumericMultiValuesSource extends MultiValuesSource<ValuesSource.Numeric> {
        public NumericMultiValuesSource(MultiValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfigs,
                                        QueryShardContext context) throws IOException {
            values = new HashMap<>(valuesSourceConfigs.getMap().size());
            for (Map.Entry<String, MultiValuesSourceConfig.Wrapper<ValuesSource.Numeric>> entry : valuesSourceConfigs.getMap().entrySet()) {
                values.put(entry.getKey(), new Wrapper<>(entry.getValue().getMulti(),
                    entry.getValue().getConfig().toValuesSource(context)));
            }
        }

        public NumericDoubleValues getField(String fieldName, LeafReaderContext ctx) throws IOException {
            Wrapper<ValuesSource.Numeric> wrapper = values.get(fieldName);
            if (wrapper == null) {
                throw new IllegalArgumentException("Could not find field name [" + fieldName + "] in multiValuesSource");
            }
            return wrapper.getMultiValueMode().select(wrapper.getValueSource().doubleValues(ctx));
        }

        public NumericDoubleValues getField(String fieldName, double defaultValue, LeafReaderContext ctx) throws IOException {
            Wrapper<ValuesSource.Numeric> wrapper = values.get(fieldName);
            if (wrapper == null) {
                throw new IllegalArgumentException("Could not find field name [" + fieldName + "] in multiValuesSource");
            }
            return FieldData.replaceMissing(wrapper.getMultiValueMode().select(wrapper.getValueSource().doubleValues(ctx)), defaultValue);
        }
    }

    public static class BytesMultiValuesSource extends MultiValuesSource<ValuesSource.Bytes> {
        public BytesMultiValuesSource(MultiValuesSourceConfig<ValuesSource.Bytes> valuesSourceConfigs,
                                      QueryShardContext context) throws IOException {
            values = new HashMap<>(valuesSourceConfigs.getMap().size());
            for (Map.Entry<String, MultiValuesSourceConfig.Wrapper<ValuesSource.Bytes>> entry : valuesSourceConfigs.getMap().entrySet()) {
                values.put(entry.getKey(), new Wrapper<>(entry.getValue().getMulti(),
                    entry.getValue().getConfig().toValuesSource(context)));
            }
        }

        public Object getField(String fieldName, LeafReaderContext ctx) throws IOException {
            Wrapper<ValuesSource.Bytes> wrapper = values.get(fieldName);
            if (wrapper == null) {
                throw new IllegalArgumentException("Could not find field name [" + fieldName + "] in multiValuesSource");
            }
            return wrapper.getValueSource().bytesValues(ctx);
        }
    }

    public static class GeoPointValuesSource extends MultiValuesSource<ValuesSource.GeoPoint> {
        public GeoPointValuesSource(MultiValuesSourceConfig<ValuesSource.GeoPoint> valuesSourceConfigs,
                                    QueryShardContext context) throws IOException {
            values = new HashMap<>(valuesSourceConfigs.getMap().size());
            for (Map.Entry<String, MultiValuesSourceConfig.Wrapper<ValuesSource.GeoPoint>> entry : valuesSourceConfigs.getMap().entrySet()){
                values.put(entry.getKey(), new Wrapper<>(entry.getValue().getMulti(),
                    entry.getValue().getConfig().toValuesSource(context)));
            }
        }
    }


    public boolean needsScores() {
        return values.values().stream().anyMatch(vsWrapper -> vsWrapper.getValueSource().needsScores());
    }

    public String[] fieldNames() {
        return values.keySet().toArray(new String[0]);
    }

    public boolean areValuesSourcesEmpty() {
        return values.values().stream().allMatch(vsWrapper -> vsWrapper.getValueSource() == null);
    }
}
