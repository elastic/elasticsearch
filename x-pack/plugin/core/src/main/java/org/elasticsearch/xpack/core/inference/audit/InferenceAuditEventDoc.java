/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.audit;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;

/**
 * Represents a single audit event for an operation performed on an inference resource (e.g., a
 * model or policy). Instances are only ever serialized to XContent for indexing into the
 * {@code .audit-inference} data stream; they are never transported between nodes.
 *
 * <p>The optional {@code resource} field carries a snapshot of the resource that was acted upon
 * (e.g. a {@link org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc} for PUT
 * events). It is stored as a {@code Map<String, Object>} so it can be both written to and parsed
 * back from XContent.
 */
public record InferenceAuditEventDoc(
    Instant timestamp,
    Action action,
    ResourceType resourceType,
    String resourceId,
    @Nullable String actor,
    @Nullable Map<String, Object> resource
) implements ToXContentObject {

    public enum Action {
        CREATE,
        UPDATE,
        DELETE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static Action fromString(String value) {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
    }

    public enum ResourceType {
        REGION_POLICY;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ResourceType fromString(String value) {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
    }

    public static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    public static final ParseField ACTION_FIELD = new ParseField("action");
    public static final ParseField RESOURCE_TYPE_FIELD = new ParseField("resource_type");
    public static final ParseField RESOURCE_ID_FIELD = new ParseField("resource_id");
    public static final ParseField ACTOR_FIELD = new ParseField("actor");
    public static final ParseField RESOURCE_FIELD = new ParseField("resource");

    public static final ConstructingObjectParser<InferenceAuditEventDoc, Void> PARSER = buildParser();

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<InferenceAuditEventDoc, Void> buildParser() {
        var parser = new ConstructingObjectParser<InferenceAuditEventDoc, Void>(
            "inference_audit_event_doc",
            true,
            args -> new InferenceAuditEventDoc(
                (Instant) args[0],
                Action.fromString((String) args[1]),
                ResourceType.fromString((String) args[2]),
                (String) args[3],
                (String) args[4],
                (Map<String, Object>) args[5]
            )
        );
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, TIMESTAMP_FIELD.getPreferredName()),
            TIMESTAMP_FIELD,
            ObjectParser.ValueType.VALUE
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), ACTION_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), RESOURCE_TYPE_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), RESOURCE_ID_FIELD);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), ACTOR_FIELD);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), RESOURCE_FIELD);
        return parser;
    }

    /**
     * Convenience constructor for audit events that carry no resource snapshot (e.g. DELETE events).
     * Equivalent to calling the full constructor with {@code resource=null}.
     */
    public InferenceAuditEventDoc(Instant timestamp, Action action, ResourceType resourceType, String resourceId, @Nullable String actor) {
        this(timestamp, action, resourceType, resourceId, actor, null);
    }

    /**
     * Creates an audit event doc from a resource that is a {@link ToXContentObject}.
     * The resource is eagerly converted to a {@code Map<String, Object>} so it can be
     * round-tripped through the XContent parser. If conversion fails the resource field
     * is set to {@code null} and the failure is surfaced to the caller.
     */
    public static InferenceAuditEventDoc withToXContentResource(
        Instant timestamp,
        Action action,
        ResourceType resourceType,
        String resourceId,
        @Nullable String actor,
        @Nullable ToXContentObject resource
    ) throws IOException {
        Map<String, Object> resourceMap = null;
        if (resource != null) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                resource.toXContent(builder, ToXContent.EMPTY_PARAMS);
                resourceMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
            }
        }
        return new InferenceAuditEventDoc(timestamp, action, resourceType, resourceId, actor, resourceMap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TIMESTAMP_FIELD.getPreferredName(), timestamp);
        builder.field(ACTION_FIELD.getPreferredName(), action);
        builder.field(RESOURCE_TYPE_FIELD.getPreferredName(), resourceType);
        builder.field(RESOURCE_ID_FIELD.getPreferredName(), resourceId);
        if (actor != null) {
            builder.field(ACTOR_FIELD.getPreferredName(), actor);
        }
        if (resource != null) {
            builder.field(RESOURCE_FIELD.getPreferredName(), resource);
        }
        builder.endObject();
        return builder;
    }

}
