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
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.field.DocValuesField;
import org.elasticsearch.script.field.ToScriptField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public class IpScriptFieldData extends BinaryScriptFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final IpFieldScript.LeafFactory leafFactory;
        private final ToScriptField<SortedBinaryDocValues> toScriptField;

        public Builder(String name, IpFieldScript.LeafFactory leafFactory, ToScriptField<SortedBinaryDocValues> toScriptField) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptField = toScriptField;
        }

        @Override
        public IpScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new IpScriptFieldData(name, leafFactory, toScriptField);
        }
    }

    private final IpFieldScript.LeafFactory leafFactory;
    private final ToScriptField<SortedBinaryDocValues> toScriptField;

    private IpScriptFieldData(String fieldName, IpFieldScript.LeafFactory leafFactory, ToScriptField<SortedBinaryDocValues> toScriptField) {
        super(fieldName);
        this.leafFactory = leafFactory;
        this.toScriptField = toScriptField;
    }

    @Override
    public BinaryScriptLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        IpFieldScript script = leafFactory.newInstance(context);
        return new BinaryScriptLeafFieldData() {
            @Override
            public DocValuesField<?> getScriptField(String name) {
                return toScriptField.getScriptField(getBytesValues(), name);
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return new org.elasticsearch.index.fielddata.IpScriptDocValues(script);
            }
        };
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.IP;
    }
}
