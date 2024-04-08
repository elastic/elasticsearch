/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class NameOrDefinition implements Writeable, ToXContentFragment {
    // exactly one of these two members is not null
    public final String name;
    public final Settings definition;

    public NameOrDefinition(String name) {
        this.name = Objects.requireNonNull(name);
        this.definition = null;
    }

    public NameOrDefinition(Map<String, ?> definition) {
        this.name = null;
        Objects.requireNonNull(definition);
        try {
            this.definition = Settings.builder().loadFromMap(definition).build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse [" + definition + "]", e);
        }
    }

    public NameOrDefinition(StreamInput in) throws IOException {
        name = in.readOptionalString();
        if (in.readBoolean()) {
            definition = Settings.readSettingsFromStream(in);
        } else {
            definition = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        boolean isNotNullDefinition = this.definition != null;
        out.writeBoolean(isNotNullDefinition);
        if (isNotNullDefinition) {
            definition.writeTo(out);
        }
    }

    public static NameOrDefinition fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new NameOrDefinition(parser.text());
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new NameOrDefinition(parser.map());
        }
        throw new XContentParseException(
            parser.getTokenLocation(),
            "Expected [VALUE_STRING] or [START_OBJECT], got " + parser.currentToken()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (definition == null) {
            builder.value(name);
        } else {
            builder.startObject();
            definition.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NameOrDefinition that = (NameOrDefinition) o;
        return Objects.equals(name, that.name) && Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, definition);
    }
}
