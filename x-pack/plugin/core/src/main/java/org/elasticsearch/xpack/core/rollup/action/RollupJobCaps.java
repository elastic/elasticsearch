/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the Rollup capabilities for a specific job on a single rollup index
 */
public class RollupJobCaps implements Writeable, ToXContentObject {
    private static ParseField JOB_ID = new ParseField("job_id");
    private static ParseField ROLLUP_INDEX = new ParseField("rollup_index");
    private static ParseField INDEX_PATTERN = new ParseField("index_pattern");
    private static ParseField FIELDS = new ParseField("fields");

    private String jobID;
    private String rollupIndex;
    private String indexPattern;
    private Map<String, RollupFieldCaps> fieldCapLookup = new HashMap<>();

    // TODO now that these rollup caps are being used more widely (e.g. search), perhaps we should
    // store the RollupJob and translate into FieldCaps on demand for json output.  Would make working with
    // it internally a lot easier
    public RollupJobCaps(RollupJobConfig job) {
        jobID = job.getId();
        rollupIndex = job.getRollupIndex();
        indexPattern = job.getIndexPattern();
        Map<String, Object> dateHistoAggCap = job.getGroupConfig().getDateHistogram().toAggCap();
        String dateField = job.getGroupConfig().getDateHistogram().getField();
        RollupFieldCaps fieldCaps = fieldCapLookup.get(dateField);
        if (fieldCaps == null) {
            fieldCaps = new RollupFieldCaps();
        }
        fieldCaps.addAgg(dateHistoAggCap);
        fieldCapLookup.put(dateField, fieldCaps);

        if (job.getGroupConfig().getHistogram() != null) {
            Map<String, Object> histoAggCap = job.getGroupConfig().getHistogram().toAggCap();
            Arrays.stream(job.getGroupConfig().getHistogram().getFields()).forEach(field -> {
                RollupFieldCaps caps = fieldCapLookup.get(field);
                if (caps == null) {
                    caps = new RollupFieldCaps();
                }
                caps.addAgg(histoAggCap);
                fieldCapLookup.put(field, caps);
            });
        }

        if (job.getGroupConfig().getTerms() != null) {
            Map<String, Object> histoAggCap = job.getGroupConfig().getTerms().toAggCap();
            Arrays.stream(job.getGroupConfig().getTerms().getFields()).forEach(field -> {
                RollupFieldCaps caps = fieldCapLookup.get(field);
                if (caps == null) {
                    caps = new RollupFieldCaps();
                }
                caps.addAgg(histoAggCap);
                fieldCapLookup.put(field, caps);
            });
        }

        if (job.getMetricsConfig().size() > 0) {
            job.getMetricsConfig().forEach(metricConfig -> {
                List<Map<String, Object>> metrics = metricConfig.toAggCap();
                metrics.forEach(m -> {
                    RollupFieldCaps caps = fieldCapLookup.get(metricConfig.getField());
                    if (caps == null) {
                        caps = new RollupFieldCaps();
                    }
                    caps.addAgg(m);
                    fieldCapLookup.put(metricConfig.getField(), caps);
                });
            });
        }
    }

    public RollupJobCaps(StreamInput in) throws IOException {
        this.jobID = in.readString();
        this.rollupIndex = in.readString();
        this.indexPattern = in.readString();
        this.fieldCapLookup = in.readMap(StreamInput::readString, RollupFieldCaps::new);
    }

    public Map<String, RollupFieldCaps> getFieldCaps() {
        return fieldCapLookup;
    }

    public String getRollupIndex() {
        return rollupIndex;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public String getJobID() {
        return jobID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobID);
        out.writeString(rollupIndex);
        out.writeString(indexPattern);
        out.writeMap(fieldCapLookup, StreamOutput::writeString, (o, value) -> value.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID.getPreferredName(), jobID);
        builder.field(ROLLUP_INDEX.getPreferredName(), rollupIndex);
        builder.field(INDEX_PATTERN.getPreferredName(), indexPattern);
        builder.startObject(FIELDS.getPreferredName());
        for (Map.Entry<String, RollupFieldCaps> fieldCap : fieldCapLookup.entrySet()) {
            builder.array(fieldCap.getKey(), fieldCap.getValue());
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RollupJobCaps that = (RollupJobCaps) other;

        return Objects.equals(this.jobID, that.jobID)
                && Objects.equals(this.rollupIndex, that.rollupIndex)
                && Objects.equals(this.fieldCapLookup, that.fieldCapLookup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobID, rollupIndex, fieldCapLookup);
    }

    public static class RollupFieldCaps implements Writeable, ToXContentObject {
        private List<Map<String, Object>> aggs = new ArrayList<>();

        RollupFieldCaps() { }

        RollupFieldCaps(StreamInput in) throws IOException {
            int size = in.readInt();
            aggs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                aggs.add(in.readMap());
            }
        }

        void addAgg(Map<String, Object> agg) {
            aggs.add(agg);
        }

        public List<Map<String, Object>> getAggs() {
            return aggs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(aggs.size());
            for (Map<String, Object> agg : aggs) {
                out.writeMap(agg);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            for (Map<String, Object> agg : aggs) {
                builder.map(agg);
            }
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            RollupFieldCaps that = (RollupFieldCaps) other;

            return Objects.equals(this.aggs, that.aggs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggs);
        }
    }
}
