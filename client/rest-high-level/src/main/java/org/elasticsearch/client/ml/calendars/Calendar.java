/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.calendars;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A simple calendar object for scheduled (special) events.
 * The calendar consists of a name an a list of job Ids or job groups
 * the events are stored separately and reference the calendar.
 */
public class Calendar implements ToXContentObject {

    public static final String CALENDAR_TYPE = "calendar";

    public static final ParseField JOB_IDS = new ParseField("job_ids");
    public static final ParseField ID = new ParseField("calendar_id");
    public static final ParseField DESCRIPTION = new ParseField("description");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Calendar, Void> PARSER =
            new ConstructingObjectParser<>(CALENDAR_TYPE, true, a ->
                    new Calendar((String) a[0], (List<String>) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), JOB_IDS);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
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
        this.jobIds = Collections.unmodifiableList(Objects.requireNonNull(jobIds, JOB_IDS.getPreferredName() + " must not be null"));
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public List<String> getJobIds() {
        return jobIds;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(JOB_IDS.getPreferredName(), jobIds);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Calendar other = (Calendar) obj;
        return id.equals(other.id) && jobIds.equals(other.jobIds) && Objects.equals(description, other.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobIds, description);
    }
}
