/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.transform;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class for methods involving arrays of transforms
 */
public class TransformConfigs extends ToXContentToBytes implements Writeable {

    public static final ParseField TRANSFORMS = new ParseField("transforms");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TransformConfigs, Void> PARSER = new ConstructingObjectParser<>(
            TRANSFORMS.getPreferredName(), a -> new TransformConfigs((List<TransformConfig>) a[0]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), TransformConfig.PARSER, TRANSFORMS);
    }

    private List<TransformConfig> transforms;

    public TransformConfigs(List<TransformConfig> transforms) {
        this.transforms = Objects.requireNonNull(transforms);
    }

    public TransformConfigs(StreamInput in) throws IOException {
        transforms = in.readList(TransformConfig::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(transforms);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TRANSFORMS.getPreferredName(), transforms);
        builder.endObject();
        return builder;
    }

    public List<TransformConfig> getTransforms() {
        return transforms;
    }

    /**
     * Set of all the field names that are required as inputs to transforms
     */
    public Set<String> inputFieldNames() {
        Set<String> fields = new HashSet<>();
        for (TransformConfig t : transforms) {
            fields.addAll(t.getInputs());
        }

        return fields;
    }

    /**
     * Set of all the field names that are outputted (i.e. created) by
     * transforms
     */
    public Set<String> outputFieldNames() {
        Set<String> fields = new HashSet<>();
        for (TransformConfig t : transforms) {
            fields.addAll(t.getOutputs());
        }

        return fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transforms);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        TransformConfigs other = (TransformConfigs) obj;
        return Objects.equals(transforms, other.transforms);
    }
}
