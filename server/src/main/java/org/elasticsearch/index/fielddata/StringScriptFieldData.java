/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.field.DocValuesField;
import org.elasticsearch.script.field.ToScriptField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public class StringScriptFieldData extends BinaryScriptFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final StringFieldScript.LeafFactory leafFactory;
        protected final ToScriptField<SortedBinaryDocValues> toScriptField;

        public Builder(String name, StringFieldScript.LeafFactory leafFactory, ToScriptField<SortedBinaryDocValues> toScriptField) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptField = toScriptField;
        }

        @Override
        public StringScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new StringScriptFieldData(name, leafFactory, toScriptField);
        }
    }

    private final StringFieldScript.LeafFactory leafFactory;
    protected final ToScriptField<SortedBinaryDocValues> toScriptField;

    private StringScriptFieldData(
        String fieldName,
        StringFieldScript.LeafFactory leafFactory,
        ToScriptField<SortedBinaryDocValues> toScriptField
    ) {
        super(fieldName);
        this.leafFactory = leafFactory;
        this.toScriptField = toScriptField;
    }

    @Override
    public BinaryScriptLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        StringFieldScript script = leafFactory.newInstance(context);
        return new BinaryScriptLeafFieldData() {
            @Override
            public DocValuesField<?> getScriptField(String name) {
                return toScriptField.getScriptField(getBytesValues(), name);
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return new StringScriptDocValues(script);
            }
        };
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }
}
