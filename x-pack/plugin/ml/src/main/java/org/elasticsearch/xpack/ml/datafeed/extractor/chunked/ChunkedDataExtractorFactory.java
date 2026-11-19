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
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;

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

    public DataExtractorFactory getDelegate() {
        return dataExtractorFactory;
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        ChunkedDataExtractorContext.TimeAligner timeAligner = newTimeAligner();
        boolean hasEsqlQuery = datafeedConfig.getEsqlQuery() != null;
        long alignedStart = alignStart(timeAligner, start);
        // End rounds down so the still-open bucket at the window edge is never extracted (real-time relies on this).
        long alignedEnd = timeAligner.alignToFloor(end);
        if (hasEsqlQuery) {
            // An unaligned raw-row start can be past the floored end; keep the window empty rather than inverted.
            alignedEnd = Math.max(alignedEnd, alignedStart);
        }
        boolean injectsEsqlRowLimit = hasEsqlQuery && EsqlDataExtractor.injectsRowLimit(datafeedConfig.getEsqlQuery());
        ChunkedDataExtractorContext dataExtractorContext = new ChunkedDataExtractorContext(
            job.getId(),
            datafeedConfig.getScrollSize(),
            alignedStart,
            alignedEnd,
            datafeedConfig.getChunkingConfig().getTimeSpan(),
            timeAligner,
            datafeedConfig.hasAggregations(),
            datafeedConfig.hasAggregations() ? datafeedConfig.getHistogramIntervalMillis(xContentRegistry) : null,
            hasEsqlQuery,
            injectsEsqlRowLimit
        );
        return new ChunkedDataExtractor(dataExtractorFactory, dataExtractorContext);
    }

    /**
     * Aggregating ES|QL floors start so the bucket containing an unaligned job start is kept (row time = bucket floor). Raw ES|QL rows
     * keep their source timestamps, so start stays as given: flooring it would re-read rows already posted before a restart.
     * DSL ceils start to skip the partial first histogram bucket. Example: 10:30 start, 1h interval — aggregating ES|QL from 10:00,
     * DSL from 11:00.
     */
    private long alignStart(ChunkedDataExtractorContext.TimeAligner timeAligner, long start) {
        String esqlQuery = datafeedConfig.getEsqlQuery();
        if (esqlQuery == null) {
            return timeAligner.alignToCeil(start);
        }
        return EsqlDataExtractor.hasAggregation(esqlQuery) ? timeAligner.alignToFloor(start) : start;
    }

    @Override
    public void excludeProject(String projectAlias) {
        dataExtractorFactory.excludeProject(projectAlias);
    }

    @Override
    public void includeProject(String projectAlias) {
        dataExtractorFactory.includeProject(projectAlias);
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
        if (datafeedConfig.getEsqlQuery() != null) {
            return newIntervalTimeAligner(datafeedConfig.getGroupingInterval().millis());
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
