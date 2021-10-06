/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents the rollup capabilities of a non-rollup index.  E.g. what values/aggregations
 * were rolled up for this index, in what rollup jobs that data is stored and where those
 * concrete rollup indices exist
 *
 * The index name can either be a single index, or an index pattern (logstash-*)
 */
public class RollableIndexCaps implements ToXContentFragment {
    private static final ParseField ROLLUP_JOBS = new ParseField("rollup_jobs");

    public static final ConstructingObjectParser<RollableIndexCaps, String> PARSER = new ConstructingObjectParser<>(
            ROLLUP_JOBS.getPreferredName(), true, (Object[] args, String indexName) -> {
                @SuppressWarnings("unchecked")
                var caps = (List<RollupJobCaps>) args[0];
                return new RollableIndexCaps(indexName, caps);
            });
    static {
        PARSER.declareObjectArray(constructorArg(), (p, name) -> RollupJobCaps.PARSER.parse(p, null), ROLLUP_JOBS);
    }

    private final String indexName;
    private final List<RollupJobCaps> jobCaps;

    RollableIndexCaps(final String indexName, final List<RollupJobCaps> caps) {
        this.indexName = indexName;
        this.jobCaps = Collections.unmodifiableList(Objects.requireNonNull(caps)
            .stream()
            .sorted(Comparator.comparing(RollupJobCaps::getJobID))
            .collect(Collectors.toList()));
    }

    public String getIndexName() {
        return indexName;
    }

    public List<RollupJobCaps> getJobCaps() {
        return jobCaps;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(indexName);
        {
            builder.field(ROLLUP_JOBS.getPreferredName(), jobCaps);
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

        RollableIndexCaps that = (RollableIndexCaps) other;
        return Objects.equals(this.jobCaps, that.jobCaps)
            && Objects.equals(this.indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobCaps, indexName);
    }
}
