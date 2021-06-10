/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the Rollup capabilities for a specific job on a single rollup index
 */
public class RollupJobCaps implements ToXContentObject {
    private static final ParseField JOB_ID = new ParseField("job_id");
    private static final ParseField ROLLUP_INDEX = new ParseField("rollup_index");
    private static final ParseField INDEX_PATTERN = new ParseField("index_pattern");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final String NAME = "rollup_job_caps";

    public static final ConstructingObjectParser<RollupJobCaps, Void> PARSER = new ConstructingObjectParser<>(NAME, true,
        a -> {
            @SuppressWarnings("unchecked")
            List<Tuple<String, RollupFieldCaps>> caps = (List<Tuple<String, RollupFieldCaps>>) a[3];
            Map<String, RollupFieldCaps> mapCaps =
                new HashMap<>(caps.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
            return new RollupJobCaps((String) a[0], (String) a[1], (String) a[2], mapCaps);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), JOB_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ROLLUP_INDEX);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_PATTERN);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(),
            (p, c, name) -> new Tuple<>(name, RollupFieldCaps.fromXContent(p)), FIELDS);
    }

    private final String jobID;
    private final String rollupIndex;
    private final String indexPattern;
    private final Map<String, RollupFieldCaps> fieldCapLookup;

    RollupJobCaps(final String jobID, final String rollupIndex,
                  final String indexPattern, final Map<String, RollupFieldCaps> fieldCapLookup) {
        this.jobID = jobID;
        this.rollupIndex = rollupIndex;
        this.indexPattern = indexPattern;
        this.fieldCapLookup = Collections.unmodifiableMap(Objects.requireNonNull(fieldCapLookup));
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
            && Objects.equals(this.indexPattern, that.indexPattern)
            && Objects.equals(this.rollupIndex, that.rollupIndex)
            && Objects.equals(this.fieldCapLookup, that.fieldCapLookup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobID, rollupIndex, fieldCapLookup, indexPattern);
    }

    public static class RollupFieldCaps implements ToXContentFragment {
        private static final String NAME = "rollup_field_caps";
        private final List<Map<String, Object>> aggs;

        RollupFieldCaps(final List<Map<String, Object>> aggs) {
            this.aggs = Collections.unmodifiableList(Objects.requireNonNull(aggs));
        }

        public List<Map<String, Object>> getAggs() {
            return aggs;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            for (Map<String, Object> agg : aggs) {
                builder.map(agg);
            }
            return builder;
        }

        public static RollupFieldCaps fromXContent(XContentParser parser) throws IOException {
            List<Map<String, Object>> aggs = new ArrayList<>();
            if (parser.nextToken().equals(XContentParser.Token.START_ARRAY)) {
                while (parser.nextToken().equals(XContentParser.Token.START_OBJECT)) {
                    aggs.add(parser.map());
                }
            }
            return new RollupFieldCaps(aggs);
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
