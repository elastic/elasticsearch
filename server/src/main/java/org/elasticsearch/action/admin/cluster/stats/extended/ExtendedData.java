/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats.extended;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExtendedData implements Writeable, ToXContent {

    private final Map<String, Map<String,Map<String,Long>>> extendedData;

    public ExtendedData() {
        this.extendedData = new HashMap<>();
    }

    public ExtendedData(Map<String, Map<String,Map<String,Long>>> extendedData) {
        this.extendedData = extendedData;
    }

    public ExtendedData(StreamInput in) throws IOException {
        this.extendedData = in.readMap(StreamInput::readString, i -> i.readMap(StreamInput::readString,
            j -> j.readMap(StreamInput::readString, StreamInput::readLong)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(extendedData, StreamOutput::writeString, (o, v)
            -> o.writeMap(v, StreamOutput::writeString, (p, q) -> p.writeMap(q, StreamOutput::writeString, StreamOutput::writeLong)));
    }

    public void merge(ExtendedData other) {
//        other.extendedData.forEach((key, otherMap) -> {
//            extendedData.merge(key, otherMap, (existingMap, newMap) -> {
//                Map<String, Map<String,Long>> mergedMap = new HashMap<>(existingMap);
//                newMap.extendedData((innerKey, innerValue) -> mergedMap.merge(innerKey, innerValue, Long::sum));
//                return mergedMap;
//            });
//        });
        other.extendedData.forEach((key, otherMap) -> {
            extendedData.merge(key, otherMap, (existingMap, newMap) -> {
                Map<String, Map<String,Long>> mergedMap = new HashMap<>(existingMap);
                newMap.forEach((innerKey, innerValue) -> {
                    mergedMap.merge(innerKey, innerValue, (existingInnerMap, newInnerMap) -> {
                        Map<String, Long> mergedInnerMap = new HashMap<>(existingInnerMap);
                        newInnerMap.forEach((propertyKey, propertyValue) -> {
                            mergedInnerMap.merge(propertyKey, propertyValue, Long::sum);
                        });
                        return mergedInnerMap;
                    });
                });
                return mergedMap;
            });
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
       for (String category : extendedData.keySet()) {
           builder.startObject(category);
           Map<String, Map<String,Long>> names = extendedData.get(category);
           for (String name : names.keySet()) {
               for (String property : names.get(name).keySet()) {
                   builder.field(property, names.get(name).get(property));
               }
           }
           builder.endObject();
       }
       builder.endObject();
       return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtendedData that = (ExtendedData) o;
        return Objects.equals(extendedData, that.extendedData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extendedData);
    }
}
