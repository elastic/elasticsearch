/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to retrieve information about model snapshots for a given job
 */
public class GetModelSnapshotsRequest implements Validatable, ToXContentObject {


    public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
    public static final ParseField SORT = new ParseField("sort");
    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField DESC = new ParseField("desc");

    public static final ConstructingObjectParser<GetModelSnapshotsRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_model_snapshots_request", a -> new GetModelSnapshotsRequest((String) a[0]));


    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(GetModelSnapshotsRequest::setSnapshotId, SNAPSHOT_ID);
        PARSER.declareString(GetModelSnapshotsRequest::setSort, SORT);
        PARSER.declareStringOrNull(GetModelSnapshotsRequest::setStart, START);
        PARSER.declareStringOrNull(GetModelSnapshotsRequest::setEnd, END);
        PARSER.declareBoolean(GetModelSnapshotsRequest::setDesc, DESC);
        PARSER.declareObject(GetModelSnapshotsRequest::setPageParams, PageParams.PARSER, PageParams.PAGE);
    }

    private final String jobId;
    private String snapshotId;
    private String sort;
    private String start;
    private String end;
    private Boolean desc;
    private PageParams pageParams;

    /**
     * Constructs a request to retrieve snapshot information from a given job
     * @param jobId id of the job from which to retrieve results
     */
    public GetModelSnapshotsRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId);
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    /**
     * Sets the id of the snapshot to retrieve.
     * @param snapshotId the snapshot id
     */
    public void setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
    }

    public String getSort() {
        return sort;
    }

    /**
     * Sets the value of "sort".
     * Specifies the snapshot field to sort on.
     * @param sort value of "sort".
     */
    public void setSort(String sort) {
        this.sort = sort;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    /**
     * Sets the paging parameters
     * @param pageParams the paging parameters
     */
    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public String getStart() {
        return start;
    }

    /**
     * Sets the value of "start" which is a timestamp.
     * Only snapshots whose timestamp is on or after the "start" value will be returned.
     * @param start String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setStart(String start) {
        this.start = start;
    }


    public String getEnd() {
        return end;
    }

    /**
     * Sets the value of "end" which is a timestamp.
     * Only snapshots whose timestamp is before the "end" value will be returned.
     * @param end String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setEnd(String end) {
        this.end = end;
    }

    public Boolean getDesc() {
        return desc;
    }

    /**
     * Sets the value of "desc".
     * Specifies the sorting order.
     * @param desc value of "desc"
     */
    public void setDesc(boolean desc) {
        this.desc = desc;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (snapshotId != null) {
            builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
        }
        if (sort != null) {
            builder.field(SORT.getPreferredName(), sort);
        }
        if (start != null) {
            builder.field(START.getPreferredName(), start);
        }
        if (end != null) {
            builder.field(END.getPreferredName(), end);
        }
        if (desc != null) {
            builder.field(DESC.getPreferredName(), desc);
        }
        if (pageParams != null) {
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
        }        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetModelSnapshotsRequest request = (GetModelSnapshotsRequest) obj;
        return Objects.equals(jobId, request.jobId)
            && Objects.equals(snapshotId, request.snapshotId)
            && Objects.equals(sort, request.sort)
            && Objects.equals(start, request.start)
            && Objects.equals(end, request.end)
            && Objects.equals(desc, request.desc)
            && Objects.equals(pageParams, request.pageParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, snapshotId, pageParams, start, end, sort, desc);
    }
}
