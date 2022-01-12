/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

interface ConfidenceValue extends NamedWriteable, ToXContentObject {

    record BucketConfidenceValue(double count, Map<String, ConfidenceValue> innerValues) implements ConfidenceValue {

        static BucketConfidenceValue fromStream(StreamInput in) throws IOException {
            return new BucketConfidenceValue(
                in.readDouble(),
                in.readMap(StreamInput::readString, (i) -> i.readNamedWriteable(ConfidenceValue.class))
            );
        }

        static final String NAME = "bucket";

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), count);
            for (var entry : innerValues.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(count);
            out.writeMap(innerValues, StreamOutput::writeString, StreamOutput::writeNamedWriteable);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

}
