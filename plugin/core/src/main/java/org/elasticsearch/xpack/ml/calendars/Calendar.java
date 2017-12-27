/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.calendars;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.MlMetaIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Calendar implements ToXContentObject, Writeable {

    public static final String CALENDAR_TYPE = "calendar";

    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField JOB_IDS = new ParseField("job_ids");
    public static final ParseField ID = new ParseField("calendar_id");

    private static final String DOCUMENT_ID_PREFIX = "calendar_";

    // For QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("calendars");

    public static final ObjectParser<Builder, Void> PARSER =
            new ObjectParser<>(ID.getPreferredName(), Calendar.Builder::new);

    static {
        PARSER.declareString(Calendar.Builder::setId, ID);
        PARSER.declareStringArray(Calendar.Builder::setJobIds, JOB_IDS);
        PARSER.declareString((builder, s) -> {}, TYPE);
    }

    public static String documentId(String calendarId) {
        return DOCUMENT_ID_PREFIX + calendarId;
    }

    private final String id;
    private final List<String> jobIds;

    public Calendar(String id, List<String> jobIds) {
        this.id = Objects.requireNonNull(id, ID.getPreferredName() + " must not be null");
        this.jobIds = Objects.requireNonNull(jobIds, JOB_IDS.getPreferredName() + " must not be null");
    }

    public Calendar(StreamInput in) throws IOException {
        id = in.readString();
        jobIds = Arrays.asList(in.readStringArray());
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeStringArray(jobIds.toArray(new String[jobIds.size()]));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(JOB_IDS.getPreferredName(), jobIds);
        if (params.paramAsBoolean(MlMetaIndex.INCLUDE_TYPE_KEY, false)) {
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

        if (!(obj instanceof Calendar)) {
            return false;
        }

        Calendar other = (Calendar) obj;
        return id.equals(other.id) && jobIds.equals(other.jobIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobIds);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String calendarId;
        private List<String> jobIds = Collections.emptyList();

        public String getId() {
            return this.calendarId;
        }

        public void setId(String calendarId) {
            this.calendarId = calendarId;
        }

        public Builder setJobIds(List<String> jobIds) {
            this.jobIds = jobIds;
            return this;
        }

        public Calendar build() {
            return new Calendar(calendarId, jobIds);
        }
    }
}
