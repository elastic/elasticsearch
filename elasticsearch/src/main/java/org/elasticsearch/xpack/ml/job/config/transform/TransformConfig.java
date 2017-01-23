/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config.transform;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.config.Condition;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an API data transform
 */
// NORELEASE: to be replaced by ingest (https://github.com/elastic/prelert-legacy/issues/39)
public class TransformConfig extends ToXContentToBytes implements Writeable {
    // Serialisation strings
    public static final ParseField TYPE = new ParseField("transform");
    public static final ParseField TRANSFORM = new ParseField("transform");
    public static final ParseField CONDITION = new ParseField("condition");
    public static final ParseField ARGUMENTS = new ParseField("arguments");
    public static final ParseField INPUTS = new ParseField("inputs");
    public static final ParseField OUTPUTS = new ParseField("outputs");

    public static final ConstructingObjectParser<TransformConfig, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(), objects -> new TransformConfig((String) objects[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
        PARSER.declareStringArray(TransformConfig::setInputs, INPUTS);
        PARSER.declareStringArray(TransformConfig::setArguments, ARGUMENTS);
        PARSER.declareStringArray(TransformConfig::setOutputs, OUTPUTS);
        PARSER.declareObject(TransformConfig::setCondition, Condition.PARSER, CONDITION);
    }

    private List<String> inputs;
    private String type;
    private List<String> arguments;
    private List<String> outputs;
    private Condition condition;

    // lazily initialized:
    private transient TransformType lazyType;

    public TransformConfig(String type) {
        this.type = type;
        lazyType = TransformType.fromString(type);
        try {
            outputs = lazyType.defaultOutputNames();
        } catch (IllegalArgumentException e) {
            outputs = Collections.emptyList();
        }
        arguments = Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public TransformConfig(StreamInput in) throws IOException {
        this(in.readString());
        inputs = (List<String>) in.readGenericValue();
        arguments = (List<String>) in.readGenericValue();
        outputs = (List<String>) in.readGenericValue();
        if (in.readBoolean()) {
            condition = new Condition(in);
        }
    }

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> fields) {
        inputs = fields;
    }

    /**
     * Transform type see {@linkplain TransformType.Names}
     */
    public String getTransform() {
        return type;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setArguments(List<String> args) {
        arguments = args;
    }

    public List<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<String> outputs) {
        this.outputs = outputs;
    }

    /**
     * The condition object which may or may not be defined for this
     * transform
     *
     * @return May be <code>null</code>
     */
    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    /**
     * This field shouldn't be serialised as its created dynamically
     * Type may be null when the class is constructed.
     */
    public TransformType type() {
        return lazyType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeGenericValue(inputs);
        out.writeGenericValue(arguments);
        out.writeGenericValue(outputs);
        if (condition != null) {
            out.writeBoolean(true);
            condition.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), type);
        if (inputs != null) {
            builder.field(INPUTS.getPreferredName(), inputs);
        }
        if (arguments != null) {
            builder.field(ARGUMENTS.getPreferredName(), arguments);
        }
        if (outputs != null) {
            builder.field(OUTPUTS.getPreferredName(), outputs);
        }
        if (condition != null) {
            builder.field(CONDITION.getPreferredName(), condition);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputs, type, outputs, arguments, condition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        TransformConfig other = (TransformConfig) obj;

        return Objects.equals(this.type, other.type)
                && Objects.equals(this.inputs, other.inputs)
                && Objects.equals(this.outputs, other.outputs)
                && Objects.equals(this.arguments, other.arguments)
                && Objects.equals(this.condition, other.condition);
    }
}
