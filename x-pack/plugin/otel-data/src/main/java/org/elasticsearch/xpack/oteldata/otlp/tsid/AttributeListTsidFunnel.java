/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.List;

class AttributeListTsidFunnel implements TsidFunnel<List<KeyValue>> {

    private final String prefix;
    private final BufferedByteStringAccessor byteStringAccessor;

    private AttributeListTsidFunnel(BufferedByteStringAccessor byteStringAccessor, String prefix) {
        this.prefix = prefix;
        this.byteStringAccessor = byteStringAccessor;
    }

    static AttributeListTsidFunnel get(BufferedByteStringAccessor byteStringAccessor, String prefix) {
        return new AttributeListTsidFunnel(byteStringAccessor, prefix);
    }

    @Override
    public void add(List<KeyValue> attributesList, TsidBuilder tsidBuilder) {
        for (int i = 0; i < attributesList.size(); i++) {
            KeyValue keyValue = attributesList.get(i);
            String attributeKey = keyValue.getKey();
            if (MetricDocumentBuilder.isIgnoredAttribute(attributeKey) == false) {
                hashValue(tsidBuilder, prefix + attributeKey, keyValue.getValue());
            }
        }
    }

    private void hashValue(TsidBuilder tsidBuilder, String key, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE:
                byteStringAccessor.addStringDimension(tsidBuilder, key, value.getStringValueBytes());
                break;
            case BOOL_VALUE:
                tsidBuilder.addBooleanDimension(key, value.getBoolValue());
                break;
            case DOUBLE_VALUE:
                tsidBuilder.addDoubleDimension(key, value.getDoubleValue());
                break;
            case INT_VALUE:
                tsidBuilder.addLongDimension(key, value.getIntValue());
                break;
            case KVLIST_VALUE:
                tsidBuilder.add(value.getKvlistValue().getValuesList(), AttributeListTsidFunnel.get(byteStringAccessor, key + "."));
                break;
            case ARRAY_VALUE:
                List<AnyValue> valuesList = value.getArrayValue().getValuesList();
                for (int i = 0; i < valuesList.size(); i++) {
                    hashValue(tsidBuilder, key, valuesList.get(i));
                }
                break;
        }
    }
}
