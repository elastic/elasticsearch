/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.ParseField;
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
