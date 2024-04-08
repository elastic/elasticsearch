/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.Objects;

public class ChunkedDataExtractorFactory implements DataExtractorFactory {

    private final DatafeedConfig datafeedConfig;
    private final Job job;
    private final DataExtractorFactory dataExtractorFactory;
    private final NamedXContentRegistry xContentRegistry;

    public ChunkedDataExtractorFactory(
        DatafeedConfig datafeedConfig,
        Job job,
        NamedXContentRegistry xContentRegistry,
        DataExtractorFactory dataExtractorFactory
    ) {
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        ChunkedDataExtractorContext.TimeAligner timeAligner = newTimeAligner();
        ChunkedDataExtractorContext dataExtractorContext = new ChunkedDataExtractorContext(
            job.getId(),
            datafeedConfig.getScrollSize(),
            timeAligner.alignToCeil(start),
            timeAligner.alignToFloor(end),
            datafeedConfig.getChunkingConfig().getTimeSpan(),
            timeAligner,
            datafeedConfig.hasAggregations(),
            datafeedConfig.hasAggregations() ? datafeedConfig.getHistogramIntervalMillis(xContentRegistry) : null
        );
        return new ChunkedDataExtractor(dataExtractorFactory, dataExtractorContext);
    }

    private ChunkedDataExtractorContext.TimeAligner newTimeAligner() {
        if (datafeedConfig.hasAggregations()) {
            // When the datafeed uses aggregations and in order to accommodate derivatives,
            // an extra bucket is queried at the beginning of each search. In order to avoid visiting
            // the same bucket twice, we need to search buckets aligned to the histogram interval.
            // This allows us to steer away from partial buckets, and thus avoid the problem of
            // dropping or duplicating data.
            return newIntervalTimeAligner(datafeedConfig.getHistogramIntervalMillis(xContentRegistry));
        }
        return newIdentityTimeAligner();
    }

    static ChunkedDataExtractorContext.TimeAligner newIdentityTimeAligner() {
        return new ChunkedDataExtractorContext.TimeAligner() {
            @Override
            public long alignToFloor(long value) {
                return value;
            }

            @Override
            public long alignToCeil(long value) {
                return value;
            }
        };
    }

    static ChunkedDataExtractorContext.TimeAligner newIntervalTimeAligner(long interval) {
        return new ChunkedDataExtractorContext.TimeAligner() {
            @Override
            public long alignToFloor(long value) {
                return Intervals.alignToFloor(value, interval);
            }

            @Override
            public long alignToCeil(long value) {
                return Intervals.alignToCeil(value, interval);
            }
        };
    }
}
