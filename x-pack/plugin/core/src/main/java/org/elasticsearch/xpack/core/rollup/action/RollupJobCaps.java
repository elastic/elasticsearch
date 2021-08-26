/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

/**
 * Represents the Rollup capabilities for a specific job on a single rollup index
 */
public class RollupJobCaps implements Writeable, ToXContentObject {
    private static ParseField JOB_ID = new ParseField("job_id");
    private static ParseField ROLLUP_INDEX = new ParseField("rollup_index");
    private static ParseField INDEX_PATTERN = new ParseField("index_pattern");
    private static ParseField FIELDS = new ParseField("fields");

    private final String jobID;
    private final String rollupIndex;
    private final String indexPattern;
    private final Map<String, RollupFieldCaps> fieldCapLookup;

    // TODO now that these rollup caps are being used more widely (e.g. search), perhaps we should
    // store the RollupJob and translate into FieldCaps on demand for json output.  Would make working with
    // it internally a lot easier
    public RollupJobCaps(RollupJobConfig job) {
        jobID = job.getId();
        rollupIndex = job.getRollupIndex();
        indexPattern = job.getIndexPattern();
        fieldCapLookup = createRollupFieldCaps(job);
    }

    public RollupJobCaps(String jobID, String rollupIndex, String indexPattern, Map<String, RollupFieldCaps> fieldCapLookup) {
        this.jobID = jobID;
        this.rollupIndex = rollupIndex;
        this.indexPattern = indexPattern;
        this.fieldCapLookup = Collections.unmodifiableMap(Objects.requireNonNull(fieldCapLookup));
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
        {
            builder.field(JOB_ID.getPreferredName(), jobID);
            builder.field(ROLLUP_INDEX.getPreferredName(), rollupIndex);
            builder.field(INDEX_PATTERN.getPreferredName(), indexPattern);
            builder.startObject(FIELDS.getPreferredName());
            {
                for (Map.Entry<String, RollupFieldCaps> fieldCap : fieldCapLookup.entrySet()) {
                    builder.array(fieldCap.getKey(), fieldCap.getValue());
                }
            }
            builder.endObject();
        }
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
        return Objects.hash(jobID, rollupIndex, fieldCapLookup, indexPattern);
    }

    private static Map<String, RollupFieldCaps> createRollupFieldCaps(final RollupJobConfig rollupJobConfig) {
        final Map<String, List<Map<String, Object>>> tempFieldCaps = new HashMap<>();

        final GroupConfig groupConfig = rollupJobConfig.getGroupConfig();
        if (groupConfig != null) {
            // Create RollupFieldCaps for the date histogram
            final DateHistogramGroupConfig dateHistogram = groupConfig.getDateHistogram();
            final Map<String, Object> dateHistogramAggCap = new HashMap<>();
            dateHistogramAggCap.put("agg", DateHistogramAggregationBuilder.NAME);
            dateHistogramAggCap.put(dateHistogram.getIntervalTypeName(), dateHistogram.getInterval().toString());

            if (dateHistogram.getDelay() != null) {
                dateHistogramAggCap.put(DateHistogramGroupConfig.DELAY, dateHistogram.getDelay().toString());
            }
            dateHistogramAggCap.put(DateHistogramGroupConfig.TIME_ZONE, dateHistogram.getTimeZone());

            List<Map<String, Object>> dateAggCaps = tempFieldCaps.getOrDefault(dateHistogram.getField(), new ArrayList<>());
            dateAggCaps.add(dateHistogramAggCap);
            tempFieldCaps.put(dateHistogram.getField(), dateAggCaps);

            // Create RollupFieldCaps for the histogram
            final HistogramGroupConfig histogram = groupConfig.getHistogram();
            if (histogram != null) {
                final Map<String, Object> histogramAggCap = new HashMap<>();
                histogramAggCap.put("agg", HistogramAggregationBuilder.NAME);
                histogramAggCap.put(HistogramGroupConfig.INTERVAL, histogram.getInterval());
                Arrays.stream(rollupJobConfig.getGroupConfig().getHistogram().getFields()).forEach(field -> {
                    List<Map<String, Object>> caps = tempFieldCaps.getOrDefault(field, new ArrayList<>());
                    caps.add(histogramAggCap);
                    tempFieldCaps.put(field, caps);
                });
            }

            // Create RollupFieldCaps for the term
            final TermsGroupConfig terms = groupConfig.getTerms();
            if (terms != null) {
                final Map<String, Object> termsAggCap = singletonMap("agg", TermsAggregationBuilder.NAME);
                Arrays.stream(rollupJobConfig.getGroupConfig().getTerms().getFields()).forEach(field -> {
                    List<Map<String, Object>> caps = tempFieldCaps.getOrDefault(field, new ArrayList<>());
                    caps.add(termsAggCap);
                    tempFieldCaps.put(field, caps);
                });
            }
        }

        // Create RollupFieldCaps for the metrics
        final List<MetricConfig> metricsConfig = rollupJobConfig.getMetricsConfig();
            if (metricsConfig.size() > 0) {
                rollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
                    final List<Map<String, Object>> metrics = metricConfig.getMetrics().stream()
                        .map(metric -> singletonMap("agg", (Object) metric))
                        .collect(Collectors.toList());
                    metrics.forEach(m -> {
                        List<Map<String, Object>> caps = tempFieldCaps
                            .getOrDefault(metricConfig.getField(), new ArrayList<>());
                        caps.add(m);
                        tempFieldCaps.put(metricConfig.getField(), caps);
                    });
                });
        }

        return Collections.unmodifiableMap(tempFieldCaps.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> new RollupFieldCaps(e.getValue()))));
    }

    public static class RollupFieldCaps implements Writeable, ToXContentFragment {
        private final List<Map<String, Object>> aggs;

        RollupFieldCaps(StreamInput in) throws IOException {
            int size = in.readInt();
            List<Map<String, Object>> inAggs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                inAggs.add(in.readMap());
            }
            this.aggs = Collections.unmodifiableList(inAggs);
        }

        RollupFieldCaps(List<Map<String, Object>> aggs) {
            this.aggs = Collections.unmodifiableList(Objects.requireNonNull(aggs));
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
