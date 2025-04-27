/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashSink;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AttributeListHashFunnel implements HashFunnel<List<KeyValue>> {

    private static final AttributeListHashFunnel INSTANCE = new AttributeListHashFunnel();
    private static final Comparator<KeyValue> COMPARATOR = Comparator.comparing(KeyValue::getKey);

    private AttributeListHashFunnel() {}

    public static AttributeListHashFunnel get() {
        return INSTANCE;
    }

    @Override
    public void put(List<KeyValue> attributesList, HashSink hashSink) {
        ArrayList<KeyValue> sortedCopy = new ArrayList<>(attributesList);
        sortedCopy.sort(COMPARATOR);
        for (int i = 0; i < sortedCopy.size(); i++) {
            KeyValue keyValue = sortedCopy.get(i);
            hashSink.putString(keyValue.getKey());
            hashValue(hashSink, keyValue.getValue());
        }
    }

    private void hashValue(HashSink hashSink, AnyValue value) {
        hashSink.putInt(value.getValueCase().getNumber());
        switch (value.getValueCase()) {
            case STRING_VALUE -> hashSink.putString(value.getStringValue());
            case BOOL_VALUE -> hashSink.putBoolean(value.getBoolValue());
            case BYTES_VALUE -> hashSink.putBytes(value.getBytesValue().toByteArray());
            case DOUBLE_VALUE -> hashSink.putDouble(value.getDoubleValue());
            case INT_VALUE -> hashSink.putLong(value.getIntValue());
            case VALUE_NOT_SET -> hashSink.putInt(0);
            case KVLIST_VALUE -> put(value.getKvlistValue().getValuesList(), hashSink);
            case ARRAY_VALUE -> {
                List<AnyValue> valuesList = value.getArrayValue().getValuesList();
                for (int i = 0; i < valuesList.size(); i++) {
                    hashValue(hashSink, valuesList.get(i));
                }
            }
        }
    }
}
