/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class AuditActivity extends ToXContentToBytes implements Writeable {
    public static final ParseField TYPE = new ParseField("audit_activity");

    public static final ParseField TOTAL_JOBS = new ParseField("total_jobs");
    public static final ParseField TOTAL_DETECTORS = new ParseField("total_detectors");
    public static final ParseField RUNNING_JOBS = new ParseField("running_jobs");
    public static final ParseField RUNNING_DETECTORS = new ParseField("running_detectors");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    public static final ObjectParser<AuditActivity, Void> PARSER = new ObjectParser<>(TYPE.getPreferredName(),
            AuditActivity::new);

    static {
        PARSER.declareInt(AuditActivity::setTotalJobs, TOTAL_JOBS);
        PARSER.declareInt(AuditActivity::setTotalDetectors, TOTAL_DETECTORS);
        PARSER.declareInt(AuditActivity::setRunningJobs, RUNNING_JOBS);
        PARSER.declareInt(AuditActivity::setRunningDetectors, RUNNING_DETECTORS);
        PARSER.declareField(AuditActivity::setTimestamp, p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + TIMESTAMP.getPreferredName() + "]");
        }, TIMESTAMP, ValueType.VALUE);
    }

    private int totalJobs;
    private int totalDetectors;
    private int runningJobs;
    private int runningDetectors;
    private Date timestamp;

    public AuditActivity() {
    }

    private AuditActivity(int totalJobs, int totalDetectors, int runningJobs, int runningDetectors) {
        this.totalJobs = totalJobs;
        this.totalDetectors = totalDetectors;
        this.runningJobs = runningJobs;
        this.runningDetectors = runningDetectors;
        timestamp = new Date();
    }

    public AuditActivity(StreamInput in) throws IOException {
        totalJobs = in.readInt();
        totalDetectors = in.readInt();
        runningJobs = in.readInt();
        runningDetectors = in.readInt();
        if (in.readBoolean()) {
            timestamp = new Date(in.readLong());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(totalJobs);
        out.writeInt(totalDetectors);
        out.writeInt(runningJobs);
        out.writeInt(runningDetectors);
        boolean hasTimestamp = timestamp != null;
        out.writeBoolean(hasTimestamp);
        if (hasTimestamp) {
            out.writeLong(timestamp.getTime());
        }
    }

    public int getTotalJobs() {
        return totalJobs;
    }

    public void setTotalJobs(int totalJobs) {
        this.totalJobs = totalJobs;
    }

    public int getTotalDetectors() {
        return totalDetectors;
    }

    public void setTotalDetectors(int totalDetectors) {
        this.totalDetectors = totalDetectors;
    }

    public int getRunningJobs() {
        return runningJobs;
    }

    public void setRunningJobs(int runningJobs) {
        this.runningJobs = runningJobs;
    }

    public int getRunningDetectors() {
        return runningDetectors;
    }

    public void setRunningDetectors(int runningDetectors) {
        this.runningDetectors = runningDetectors;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public static AuditActivity newActivity(int totalJobs, int totalDetectors, int runningJobs, int runningDetectors) {
        return new AuditActivity(totalJobs, totalDetectors, runningJobs, runningDetectors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL_JOBS.getPreferredName(), totalJobs);
        builder.field(TOTAL_DETECTORS.getPreferredName(), totalDetectors);
        builder.field(RUNNING_JOBS.getPreferredName(), runningJobs);
        builder.field(RUNNING_DETECTORS.getPreferredName(), runningDetectors);
        if (timestamp != null) {
            builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalDetectors, totalJobs, runningDetectors, runningJobs, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AuditActivity other = (AuditActivity) obj;
        return Objects.equals(totalDetectors, other.totalDetectors) &&
                Objects.equals(totalJobs, other.totalJobs) &&
                Objects.equals(runningDetectors, other.runningDetectors) &&
                Objects.equals(runningJobs, other.runningJobs) &&
                Objects.equals(timestamp, other.timestamp);
    }
}
