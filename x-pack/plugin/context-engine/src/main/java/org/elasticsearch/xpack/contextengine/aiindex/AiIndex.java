/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * One AI index registry record: the Kibana-space-scoped metadata that points at a backing store and describes how the
 * AI index is fed and analyzed. The backing store itself is an ordinary index or data stream created from the AI index
 * templates; this record only references it.
 */
public class AiIndex implements Writeable, ToXContentObject {

    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField SPACE_FIELD = new ParseField("space");
    public static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    public static final ParseField MANAGED_FIELD = new ParseField("managed");
    public static final ParseField MEMORY_ENABLED_FIELD = new ParseField("memory_enabled");
    public static final ParseField DATE_CREATED_FIELD = new ParseField("date_created");
    public static final ParseField DATE_MODIFIED_FIELD = new ParseField("date_modified");
    public static final ParseField DEST_FIELD = new ParseField("dest");
    public static final ParseField AUTOMATIONS_FIELD = new ParseField("automations");
    public static final ParseField SOURCES_FIELD = new ParseField("sources");
    public static final ParseField TRACES_FIELD = new ParseField("traces");
    public static final ParseField FEEDBACK_ANALYSIS_FIELD = new ParseField("feedback_analysis");

    public static final String DEST_TYPE_INDEX = "index";
    public static final String DEST_TYPE_DATA_STREAM = "data_stream";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AiIndex, Void> PARSER = new ConstructingObjectParser<>(
        "ai_index",
        args -> new AiIndex(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            args[3] != null && (Boolean) args[3],
            (Boolean) args[4],
            args[5] == null ? null : Instant.parse((String) args[5]),
            args[6] == null ? null : Instant.parse((String) args[6]),
            (TypeValue) args[7],
            (List<TypeValue>) args[8],
            (List<TypeValue>) args[9],
            (List<TypeValue>) args[10],
            (Map<String, Object>) args[11]
        )
    );

    static {
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), SPACE_FIELD);
        PARSER.declareString(optionalConstructorArg(), DESCRIPTION_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), MANAGED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), MEMORY_ENABLED_FIELD);
        PARSER.declareString(optionalConstructorArg(), DATE_CREATED_FIELD);
        PARSER.declareString(optionalConstructorArg(), DATE_MODIFIED_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> TypeValue.fromXContent(p), DEST_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> TypeValue.fromXContent(p), AUTOMATIONS_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> TypeValue.fromXContent(p), SOURCES_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> TypeValue.fromXContent(p), TRACES_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), FEEDBACK_ANALYSIS_FIELD);
    }

    public static AiIndex fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Nullable
    private final String id;
    @Nullable
    private final String space;
    @Nullable
    private final String description;
    private final boolean managed;
    @Nullable
    private final Boolean memoryEnabled;
    @Nullable
    private final Instant dateCreated;
    @Nullable
    private final Instant dateModified;
    @Nullable
    private final TypeValue dest;
    private final List<TypeValue> automations;
    private final List<TypeValue> sources;
    private final List<TypeValue> traces;
    @Nullable
    private final Map<String, Object> feedbackAnalysis;

    public AiIndex(
        @Nullable String id,
        @Nullable String space,
        @Nullable String description,
        boolean managed,
        @Nullable Boolean memoryEnabled,
        @Nullable Instant dateCreated,
        @Nullable Instant dateModified,
        @Nullable TypeValue dest,
        @Nullable List<TypeValue> automations,
        @Nullable List<TypeValue> sources,
        @Nullable List<TypeValue> traces,
        @Nullable Map<String, Object> feedbackAnalysis
    ) {
        this.id = id;
        this.space = space;
        this.description = description;
        this.managed = managed;
        this.memoryEnabled = memoryEnabled;
        this.dateCreated = dateCreated;
        this.dateModified = dateModified;
        this.dest = dest;
        this.automations = automations == null ? List.of() : List.copyOf(automations);
        this.sources = sources == null ? List.of() : List.copyOf(sources);
        this.traces = traces == null ? List.of() : List.copyOf(traces);
        this.feedbackAnalysis = feedbackAnalysis;
    }

    public AiIndex(StreamInput in) throws IOException {
        this.id = in.readOptionalString();
        this.space = in.readOptionalString();
        this.description = in.readOptionalString();
        this.managed = in.readBoolean();
        this.memoryEnabled = in.readOptionalBoolean();
        this.dateCreated = in.readOptionalInstant();
        this.dateModified = in.readOptionalInstant();
        this.dest = in.readOptionalWriteable(TypeValue::new);
        this.automations = in.readCollectionAsImmutableList(TypeValue::new);
        this.sources = in.readCollectionAsImmutableList(TypeValue::new);
        this.traces = in.readCollectionAsImmutableList(TypeValue::new);
        this.feedbackAnalysis = in.readBoolean() ? in.readGenericMap() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(id);
        out.writeOptionalString(space);
        out.writeOptionalString(description);
        out.writeBoolean(managed);
        out.writeOptionalBoolean(memoryEnabled);
        out.writeOptionalInstant(dateCreated);
        out.writeOptionalInstant(dateModified);
        out.writeOptionalWriteable(dest);
        out.writeCollection(automations);
        out.writeCollection(sources);
        out.writeCollection(traces);
        out.writeBoolean(feedbackAnalysis != null);
        if (feedbackAnalysis != null) {
            out.writeGenericMap(feedbackAnalysis);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field(ID_FIELD.getPreferredName(), id);
        }
        if (space != null) {
            builder.field(SPACE_FIELD.getPreferredName(), space);
        }
        if (description != null) {
            builder.field(DESCRIPTION_FIELD.getPreferredName(), description);
        }
        builder.field(MANAGED_FIELD.getPreferredName(), managed);
        if (memoryEnabled != null) {
            builder.field(MEMORY_ENABLED_FIELD.getPreferredName(), memoryEnabled);
        }
        if (dateCreated != null) {
            builder.field(DATE_CREATED_FIELD.getPreferredName(), dateCreated.toString());
        }
        if (dateModified != null) {
            builder.field(DATE_MODIFIED_FIELD.getPreferredName(), dateModified.toString());
        }
        if (dest != null) {
            builder.field(DEST_FIELD.getPreferredName(), dest);
        }
        builder.xContentList(AUTOMATIONS_FIELD.getPreferredName(), automations);
        builder.xContentList(SOURCES_FIELD.getPreferredName(), sources);
        builder.xContentList(TRACES_FIELD.getPreferredName(), traces);
        if (feedbackAnalysis != null) {
            builder.field(FEEDBACK_ANALYSIS_FIELD.getPreferredName(), feedbackAnalysis);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns a copy with the identity and timestamps the registry assigns when storing the record.
     */
    public AiIndex withIdentityAndDates(String id, String space, Instant dateCreated, Instant dateModified) {
        return new AiIndex(
            id,
            space,
            description,
            managed,
            memoryEnabled,
            dateCreated,
            dateModified,
            dest,
            automations,
            sources,
            traces,
            feedbackAnalysis
        );
    }

    @Nullable
    public String id() {
        return id;
    }

    @Nullable
    public String space() {
        return space;
    }

    @Nullable
    public String description() {
        return description;
    }

    public boolean managed() {
        return managed;
    }

    @Nullable
    public Boolean memoryEnabled() {
        return memoryEnabled;
    }

    @Nullable
    public Instant dateCreated() {
        return dateCreated;
    }

    @Nullable
    public Instant dateModified() {
        return dateModified;
    }

    @Nullable
    public TypeValue dest() {
        return dest;
    }

    public List<TypeValue> automations() {
        return automations;
    }

    public List<TypeValue> sources() {
        return sources;
    }

    public List<TypeValue> traces() {
        return traces;
    }

    @Nullable
    public Map<String, Object> feedbackAnalysis() {
        return feedbackAnalysis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AiIndex that = (AiIndex) o;
        return managed == that.managed
            && Objects.equals(id, that.id)
            && Objects.equals(space, that.space)
            && Objects.equals(description, that.description)
            && Objects.equals(memoryEnabled, that.memoryEnabled)
            && Objects.equals(dateCreated, that.dateCreated)
            && Objects.equals(dateModified, that.dateModified)
            && Objects.equals(dest, that.dest)
            && Objects.equals(automations, that.automations)
            && Objects.equals(sources, that.sources)
            && Objects.equals(traces, that.traces)
            && Objects.equals(feedbackAnalysis, that.feedbackAnalysis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            space,
            description,
            managed,
            memoryEnabled,
            dateCreated,
            dateModified,
            dest,
            automations,
            sources,
            traces,
            feedbackAnalysis
        );
    }
}
