/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates enrich policies as custom metadata inside cluster state.
 */
public final class EnrichMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    static final String TYPE = "enrich";

    static final ParseField POLICIES = new ParseField("policies");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnrichMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "enrich_metadata",
        args -> new EnrichMetadata((Map<String, EnrichPolicy>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, EnrichPolicy> patterns = new HashMap<>();
            String fieldName = null;
            for (XContentParser.Token token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = p.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    patterns.put(fieldName, EnrichPolicy.fromXContent(p));
                } else {
                    throw new ElasticsearchParseException("unexpected token [" + token + "]");
                }
            }
            return patterns;
        }, POLICIES);
    }

    public static EnrichMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, EnrichPolicy> policies;

    public EnrichMetadata(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, EnrichPolicy::new));
    }

    public EnrichMetadata(Map<String, EnrichPolicy> policies) {
        this.policies = Collections.unmodifiableMap(policies);
    }

    public Map<String, EnrichPolicy> getPolicies() {
        return policies;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_5_0;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(policies, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(POLICIES.getPreferredName());
        for (Map.Entry<String, EnrichPolicy> entry : policies.entrySet()) {
            builder.startObject(entry.getKey());
            builder.value(entry.getValue());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichMetadata that = (EnrichMetadata) o;
        return policies.equals(that.policies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policies);
    }

}
