/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DestAlias implements Writeable, ToXContentObject {

    public static final ParseField ALIAS = new ParseField("alias");
    public static final ParseField MOVE_ON_CREATION = new ParseField("move_on_creation");

    public static final ConstructingObjectParser<DestAlias, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<DestAlias, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<DestAlias, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DestAlias, Void> parser = new ConstructingObjectParser<>(
            "data_frame_config_dest_alias",
            lenient,
            args -> new DestAlias((String) args[0], (Boolean) args[1])
        );
        parser.declareString(constructorArg(), ALIAS);
        parser.declareBoolean(optionalConstructorArg(), MOVE_ON_CREATION);
        return parser;
    }

    private final String alias;
    private final boolean moveOnCreation;

    public DestAlias(String alias, Boolean moveOnCreation) {
        this.alias = ExceptionsHelper.requireNonNull(alias, ALIAS.getPreferredName());
        this.moveOnCreation = moveOnCreation != null ? moveOnCreation : false;
    }

    public DestAlias(final StreamInput in) throws IOException {
        alias = in.readString();
        moveOnCreation = in.readBoolean();
    }

    public String getAlias() {
        return alias;
    }

    public boolean isMoveOnCreation() {
        return moveOnCreation;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (alias.isEmpty()) {
            validationException = addValidationError("dest.aliases.alias must not be empty", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias);
        out.writeBoolean(moveOnCreation);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ALIAS.getPreferredName(), alias);
        builder.field(MOVE_ON_CREATION.getPreferredName(), moveOnCreation);
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

        DestAlias that = (DestAlias) other;
        return Objects.equals(alias, that.alias) && moveOnCreation == that.moveOnCreation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, moveOnCreation);
    }

    public static DestAlias fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
