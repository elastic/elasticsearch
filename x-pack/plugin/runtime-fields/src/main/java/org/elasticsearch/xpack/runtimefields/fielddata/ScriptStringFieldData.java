/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

public class ScriptStringFieldData extends ScriptBinaryFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final StringScriptFieldScript.LeafFactory leafFactory;

        public Builder(String name, StringScriptFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public ScriptStringFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new ScriptStringFieldData(name, leafFactory);
        }
    }

    private final StringScriptFieldScript.LeafFactory leafFactory;

    private ScriptStringFieldData(String fieldName, StringScriptFieldScript.LeafFactory leafFactory) {
        super(fieldName);
        this.leafFactory = leafFactory;
    }

    @Override
    public ScriptBinaryLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        StringScriptFieldScript script = leafFactory.newInstance(context);
        return new ScriptBinaryLeafFieldData() {
            @Override
            public ScriptDocValues<?> getScriptValues() {
                return new ScriptDocValues.Strings(getBytesValues());
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return new ScriptStringDocValues(script);
            }
        };
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.BYTES;
    }
}
