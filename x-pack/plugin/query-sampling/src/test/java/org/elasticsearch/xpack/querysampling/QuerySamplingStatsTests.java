/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingStatsTests extends AbstractWireSerializingTestCase<QuerySamplingStats> {

    @Override
    protected Writeable.Reader<QuerySamplingStats> instanceReader() {
        return QuerySamplingStats::new;
    }

    @Override
    protected QuerySamplingStats createTestInstance() {
        return new QuerySamplingStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected QuerySamplingStats mutateInstance(QuerySamplingStats instance) {
        long[] values = {
            instance.knnSearches(),
            instance.captured(),
            instance.dropped(),
            instance.distinctQueries(),
            instance.untrackedArrivals(),
            instance.picked(),
            instance.buffered(),
            instance.withGroundTruth(),
            instance.rejected() };
        values[between(0, values.length - 1)]++;
        return new QuerySamplingStats(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]);
    }

    public void testRendersEveryCounter() throws IOException {
        QuerySamplingStats stats = new QuerySamplingStats(1, 2, 3, 4, 5, 6, 7, 8, 9);
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(
            Strings.toString(builder),
            equalTo(
                "{\"knn_searches\":1,\"captured\":2,\"dropped\":3,\"distinct_queries\":4,"
                    + "\"untracked_arrivals\":5,\"picked\":6,\"buffered\":7,\"with_ground_truth\":8,\"rejected\":9}"
            )
        );
    }
}
