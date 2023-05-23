/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DestConfig implements Writeable, ToXContentObject {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField ALIASES = new ParseField("aliases");
    public static final ParseField PIPELINE = new ParseField("pipeline");

    public static final ConstructingObjectParser<DestConfig, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<DestConfig, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<DestConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DestConfig, Void> parser = new ConstructingObjectParser<>(
            "data_frame_config_dest",
            lenient,
            args -> new DestConfig((String) args[0], (List<DestAlias>) args[1], (String) args[2])
        );
        parser.declareString(constructorArg(), INDEX);
        parser.declareObjectArray(optionalConstructorArg(), lenient ? DestAlias.LENIENT_PARSER : DestAlias.STRICT_PARSER, ALIASES);
        parser.declareString(optionalConstructorArg(), PIPELINE);
        return parser;
    }

    private final String index;
    private final List<DestAlias> aliases;
    private final String pipeline;

    public DestConfig(String index, List<DestAlias> aliases, String pipeline) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX.getPreferredName());
        this.aliases = aliases;
        this.pipeline = pipeline;
    }

    public DestConfig(final StreamInput in) throws IOException {
        index = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            aliases = in.readOptionalList(DestAlias::new);
        } else {
            aliases = null;
        }
        pipeline = in.readOptionalString();
    }

    public String getIndex() {
        return index;
    }

    public List<DestAlias> getAliases() {
        return aliases != null ? aliases : List.of();
    }

    public String getPipeline() {
        return pipeline;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (index.isEmpty()) {
            validationException = addValidationError("dest.index must not be empty", validationException);
        }
        return validationException;
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeOptionalCollection(aliases);
        }
        out.writeOptionalString(pipeline);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        if (aliases != null) {
            builder.field(ALIASES.getPreferredName(), aliases);
        }
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
        return Objects.equals(index, that.index) && Objects.equals(aliases, that.aliases) && Objects.equals(pipeline, that.pipeline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, aliases, pipeline);
    }

    public static DestConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
