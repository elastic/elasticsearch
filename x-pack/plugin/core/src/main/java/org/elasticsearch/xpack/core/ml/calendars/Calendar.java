/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.calendars;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A simple calendar object for scheduled (special) events.
 * The calendar consists of a name an a list of job Ids or job groups.
 */
public class Calendar implements ToXContentObject, Writeable {

    public static final String CALENDAR_TYPE = "calendar";

    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField JOB_IDS = new ParseField("job_ids");
    public static final ParseField ID = new ParseField("calendar_id");
    public static final ParseField DESCRIPTION = new ParseField("description");

    private static final String DOCUMENT_ID_PREFIX = "calendar_";

    // For QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("calendars");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(ID.getPreferredName(), ignoreUnknownFields, Builder::new);

        parser.declareString(Builder::setId, ID);
        parser.declareStringArray(Builder::setJobIds, JOB_IDS);
        parser.declareString((builder, s) -> {}, TYPE);
        parser.declareStringOrNull(Builder::setDescription, DESCRIPTION);

        return parser;
    }

    public static String documentId(String calendarId) {
        return DOCUMENT_ID_PREFIX + calendarId;
    }

    private final String id;
    private final List<String> jobIds;
    private final String description;

    /**
     * {@code jobIds} can be a mix of job groups and job Ids
     * @param id The calendar Id
     * @param jobIds List of job Ids or job groups
     * @param description An optional description
     */
    public Calendar(String id, List<String> jobIds, @Nullable String description) {
        this.id = Objects.requireNonNull(id, ID.getPreferredName() + " must not be null");
        this.jobIds = Objects.requireNonNull(jobIds, JOB_IDS.getPreferredName() + " must not be null");
        this.description = description;
    }

    public Calendar(StreamInput in) throws IOException {
        id = in.readString();
        jobIds = Arrays.asList(in.readStringArray());
        description = in.readOptionalString();
    }

    public String getId() {
        return id;
    }

    public String documentId() {
        return documentId(id);
    }

    public List<String> getJobIds() {
        return Collections.unmodifiableList(jobIds);
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeStringArray(jobIds.toArray(new String[jobIds.size()]));
        out.writeOptionalString(description);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(JOB_IDS.getPreferredName(), jobIds);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(TYPE.getPreferredName(), CALENDAR_TYPE);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if ((obj instanceof Calendar) == false) {
            return false;
        }

        Calendar other = (Calendar) obj;
        return id.equals(other.id) && jobIds.equals(other.jobIds) && Objects.equals(description, other.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobIds, description);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String calendarId;
        private List<String> jobIds = Collections.emptyList();
        private String description;

        public String getId() {
            return calendarId;
        }

        public void setId(String calendarId) {
            this.calendarId = calendarId;
        }

        public Builder setJobIds(List<String> jobIds) {
            this.jobIds = jobIds;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Calendar build() {
            return new Calendar(calendarId, jobIds, description);
        }
    }
}
