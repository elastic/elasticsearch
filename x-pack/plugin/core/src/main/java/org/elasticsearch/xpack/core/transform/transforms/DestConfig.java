/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DestConfig implements Writeable, ToXContentObject {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField PIPELINE = new ParseField("pipeline");

    public static final ConstructingObjectParser<DestConfig, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<DestConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<DestConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DestConfig, Void> parser = new ConstructingObjectParser<>("data_frame_config_dest",
            lenient,
            args -> new DestConfig((String)args[0], (String) args[1]));
        parser.declareString(constructorArg(), INDEX);
        parser.declareString(optionalConstructorArg(), PIPELINE);
        return parser;
    }

    private final String index;
    private final String pipeline;

    public DestConfig(String index, String pipeline) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX.getPreferredName());
        this.pipeline = pipeline;
    }

    public DestConfig(final StreamInput in) throws IOException {
        index = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            pipeline = in.readOptionalString();
        } else {
            pipeline = null;
        }
    }

    public String getIndex() {
        return index;
    }

    public String getPipeline() {
        return pipeline;
    }

    public boolean isValid() {
        return index.isEmpty() == false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalString(pipeline);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        if (pipeline != null) {
            builder.field(PIPELINE.getPreferredName(), pipeline);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        DestConfig that = (DestConfig) other;
        return Objects.equals(index, that.index) &&
            Objects.equals(pipeline, that.pipeline);
    }

    @Override
    public int hashCode(){
        return Objects.hash(index, pipeline);
    }

    public static DestConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
