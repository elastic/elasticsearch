/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates enrich policies as custom metadata inside cluster state.
 */
public final class EnrichMetadata extends AbstractNamedDiffable<MetaData.Custom> implements XPackPlugin.XPackMetaDataCustom {

    static final String TYPE = "enrich";

    static final ParseField POLICIES = new ParseField("policies");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnrichMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "enrich_metadata",
        args -> new EnrichMetadata((Map<String, Policy>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, Policy> patterns = new HashMap<>();
            String fieldName = null;
            for (XContentParser.Token token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = p.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    patterns.put(fieldName, Policy.PARSER.parse(p, c));
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

    private final Map<String, Policy> policies;

    public EnrichMetadata(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, Policy::new));
    }

    public EnrichMetadata(Map<String, Policy> policies) {
        this.policies = Collections.unmodifiableMap(policies);
    }

    public Map<String, Policy> getPolicies() {
        return policies;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        // NO RELEASE: change when merging enrich & enrich-7.x into master and 7.x respectively:
        return Version.V_7_1_0;
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
        for (Map.Entry<String, Policy> entry : policies.entrySet()) {
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

    /**
     * Represents an enrich policy including its configuration.
     */
    public static final class Policy implements Writeable, ToXContentObject {

        static final ParseField TYPE = new ParseField("type");
        static final ParseField UPDATE_INTERVAL = new ParseField("update_interval");
        static final ParseField SOURCE_INDEX = new ParseField("source_index");
        static final ParseField QUERY_FIELD = new ParseField("query_field");
        static final ParseField DECORATE_FIELDS = new ParseField("decorate_fields");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Policy, Void> PARSER = new ConstructingObjectParser<>(
            "policy",
            args -> {
                return new Policy(Type.read((String) args[0]), (TimeValue) args[1], (String) args[2], (String) args[3],
                    (List<String>) args[4]);
            }
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), UPDATE_INTERVAL.getPreferredName()),
                UPDATE_INTERVAL, ObjectParser.ValueType.STRING);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), SOURCE_INDEX);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), DECORATE_FIELDS);
        }

        private final Type type;
        private final TimeValue updateInterval;
        private final String sourceIndex;
        private final String queryField;
        private final List<String> decorateFields;

        public Policy(StreamInput in) throws IOException {
            this(Type.read(in.readString()), in.readTimeValue(), in.readString(), in.readString(), in.readStringList());
        }

        public Policy(Type type,
                      TimeValue updateInterval,
                      String sourceIndex,
                      String queryField,
                      List<String> decorateFields) {
            this.type = type;
            this.updateInterval = updateInterval;
            this.sourceIndex = sourceIndex;
            this.queryField = queryField;
            this.decorateFields = decorateFields;
        }

        public Type getType() {
            return type;
        }

        public TimeValue getUpdateInterval() {
            return updateInterval;
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getQueryField() {
            return queryField;
        }

        public List<String> getDecorateFields() {
            return decorateFields;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type.toString());
            out.writeTimeValue(updateInterval);
            out.writeString(sourceIndex);
            out.writeString(queryField);
            out.writeStringCollection(decorateFields);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TYPE.getPreferredName(), type.toString());
            builder.field(UPDATE_INTERVAL.getPreferredName(), updateInterval.getStringRep());
            builder.field(SOURCE_INDEX.getPreferredName(), sourceIndex);
            builder.field(QUERY_FIELD.getPreferredName(), queryField);
            builder.array(DECORATE_FIELDS.getPreferredName(), decorateFields.toArray(new String[0]));
            return builder;
        }

        @Override
        public boolean isFragment() {
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Policy policy = (Policy) o;
            return type == policy.type &&
                updateInterval.equals(policy.updateInterval) &&
                sourceIndex.equals(policy.sourceIndex) &&
                queryField.equals(policy.queryField) &&
                decorateFields.equals(policy.decorateFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, updateInterval, sourceIndex, queryField, decorateFields);
        }

        public enum Type {

            STRING;

            public static Type read(String in) {
                switch (in) {
                    case "string":
                        return STRING;
                    default:
                        throw new IllegalArgumentException("unknown value [" + in + "]");
                }
            }

            @Override
            public String toString() {
                return super.toString().toLowerCase(Locale.ROOT);
            }
        }
    }
}
