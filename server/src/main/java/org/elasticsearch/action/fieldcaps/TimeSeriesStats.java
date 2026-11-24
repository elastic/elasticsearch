/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.io.IOException;
import java.time.ZoneId;

/**
 * Estimated time-series stats of an index.
 * Across indices, the total of {@code seriesMillis} divided by the union of their {@code [startMillis, endMillis]} estimates the series.
 *
 * @param seriesMillis time series * time range in milliseconds.
 *                     For example, 10 series over one minute is 600,000; with 10-second buckets that is 60 {@code (_tsid, bucket)} groups.
 * @param startMillis  the first covered {@code @timestamp} in epoch milliseconds
 * @param endMillis    the last covered {@code @timestamp} in epoch milliseconds.
 *
 */
public record TimeSeriesStats(double seriesMillis, long startMillis, long endMillis) implements Writeable {

    public TimeSeriesStats(StreamInput in) throws IOException {
        this(in.readDouble(), in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(seriesMillis);
        out.writeLong(startMillis);
        out.writeLong(endMillis);
    }

    @Nullable
    static TimeSeriesStats ofShard(
        IndexReader reader,
        DateFieldMapper.Resolution resolution,
        int numberOfShards,
        long fromMillis,
        long toMillis
    ) throws IOException {
        double seriesMillis = 0;
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        for (LeafReaderContext leaf : reader.leaves()) {
            final SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            if (tsid == null) {
                continue;
            }
            final long[] range = timestampRange(leaf.reader(), resolution);
            if (range == null) {
                return null;
            }
            final long from = Math.max(range[0], fromMillis);
            final long to = Math.min(range[1], toMillis);
            if (from > to) {
                continue;
            }
            seriesMillis += tsid.getValueCount() * (double) (to - from + 1);
            minTimestamp = Math.min(minTimestamp, from);
            maxTimestamp = Math.max(maxTimestamp, to);
        }
        return new TimeSeriesStats(seriesMillis * numberOfShards, minTimestamp, maxTimestamp);
    }

    static long[] timestampBounds(@Nullable QueryBuilder filter, DateFieldMapper.DateFieldType timestamp, long nowInMillis) {
        final long[] bounds = { Long.MIN_VALUE, Long.MAX_VALUE };
        try {
            collectTimestampBounds(filter, timestamp, nowInMillis, bounds);
        } catch (RuntimeException unused) {
            // ignored invalid time bounds
            return null;
        }
        return bounds;
    }

    private static void collectTimestampBounds(
        @Nullable QueryBuilder filter,
        DateFieldMapper.DateFieldType timestamp,
        long nowInMillis,
        long[] bounds
    ) {
        if (filter instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.filter()) {
                collectTimestampBounds(clause, timestamp, nowInMillis, bounds);
            }
            for (QueryBuilder clause : bool.must()) {
                collectTimestampBounds(clause, timestamp, nowInMillis, bounds);
            }
        } else if (filter instanceof RangeQueryBuilder range && range.fieldName().equals(timestamp.name())) {
            final DateMathParser parser = timestamp.resolveDateMathParser(
                Strings.hasText(range.format()) ? DateFormatter.forPattern(range.format()).toDateMathParser() : null,
                range.from(),
                range.to()
            );
            final ZoneId zone = range.timeZone() == null ? null : ZoneId.of(range.timeZone());
            final DateFieldMapper.Resolution resolution = timestamp.resolution();
            if (range.from() != null) {
                long from = timestamp.parseToLong(range.from(), range.includeLower() == false, zone, parser, () -> nowInMillis);
                if (range.includeLower() == false) {
                    from++;
                }
                bounds[0] = Math.max(bounds[0], resolution.roundDownToMillis(from));
            }
            if (range.to() != null) {
                long to = timestamp.parseToLong(range.to(), range.includeUpper(), zone, parser, () -> nowInMillis);
                if (range.includeUpper() == false) {
                    to--;
                }
                bounds[1] = Math.min(bounds[1], resolution.roundUpToMillis(to));
            }
        }
    }

    private static long[] timestampRange(LeafReader reader, DateFieldMapper.Resolution resolution) throws IOException {
        final long min;
        final long max;
        final DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
        if (skipper != null) {
            min = skipper.minValue();
            max = skipper.maxValue();
        } else {
            final PointValues points = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
            if (points == null) {
                return null;
            }
            min = LongPoint.decodeDimension(points.getMinPackedValue(), 0);
            max = LongPoint.decodeDimension(points.getMaxPackedValue(), 0);
        }
        return new long[] { resolution.roundDownToMillis(min), resolution.roundUpToMillis(max) };
    }
}
