/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

public class EnrichPolicy implements Writeable, ToXContentFragment {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField VERSION_CREATED = new ParseField("version_created");
    private static final ParseField DEFINITION = new ParseField("definition");

    private static final ConstructingObjectParser<EnrichPolicy, Void> PARSER = new ConstructingObjectParser<>("policy",
        args -> new EnrichPolicy(
            (String) args[0],
            (Version) args[1],
            (EnrichPolicyDefinition) args[2]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> Version.fromString(p.text()), VERSION_CREATED,
            ObjectParser.ValueType.STRING);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> EnrichPolicyDefinition.fromXContent(p), DEFINITION);
    }

    public static EnrichPolicy fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String name;
    private final Version versionCreated;
    private final EnrichPolicyDefinition definition;

    public EnrichPolicy(StreamInput in) throws IOException {
        this(
            in.readString(),
            Version.readVersion(in),
            new EnrichPolicyDefinition(in)
        );
    }

    public EnrichPolicy(String name, Version versionCreated, EnrichPolicyDefinition definition) {
        this.name = name;
        this.versionCreated = versionCreated;
        this.definition = definition;
    }

    public String getName() {
        return name;
    }

    public Version getVersionCreated() {
        return versionCreated;
    }

    public EnrichPolicyDefinition getDefinition() {
        return definition;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        Version.writeVersion(versionCreated, out);
        definition.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME.getPreferredName(), name);
        builder.field(VERSION_CREATED.getPreferredName());
        versionCreated.toXContent(builder, params);
        builder.startObject(DEFINITION.getPreferredName());
        definition.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichPolicy policy = (EnrichPolicy) o;
        return name.equals(policy.name) &&
            Objects.equals(versionCreated, policy.versionCreated) &&
            Objects.equals(definition, policy.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, versionCreated, definition);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
