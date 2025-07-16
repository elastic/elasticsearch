/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

record DatetimeEmptyBucketGenerator(long from, long to, Rounding.Prepared rounding) implements BlockHash.EmptyBucketGenerator {

    // TODO maybe we should just cover the whole of representable dates here - like ten years, 100 years, 1000 years, all the way up.
    // That way you never end up with more than the target number of buckets.
    private static final Rounding LARGEST_HUMAN_DATE_ROUNDING = Rounding.builder(Rounding.DateTimeUnit.YEAR_OF_CENTURY).build();
    private static final Rounding[] HUMAN_DATE_ROUNDINGS = new Rounding[] {
        Rounding.builder(Rounding.DateTimeUnit.MONTH_OF_YEAR).build(),
        Rounding.builder(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR).build(),
        Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build(),
        Rounding.builder(TimeValue.timeValueHours(12)).build(),
        Rounding.builder(TimeValue.timeValueHours(3)).build(),
        Rounding.builder(TimeValue.timeValueHours(1)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(30)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(10)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(5)).build(),
        Rounding.builder(TimeValue.timeValueMinutes(1)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(30)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(10)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(5)).build(),
        Rounding.builder(TimeValue.timeValueSeconds(1)).build(),
        Rounding.builder(TimeValue.timeValueMillis(100)).build(),
        Rounding.builder(TimeValue.timeValueMillis(50)).build(),
        Rounding.builder(TimeValue.timeValueMillis(10)).build(),
        Rounding.builder(TimeValue.timeValueMillis(1)).build(), };

    private static final ZoneId DEFAULT_TZ = ZoneOffset.UTC; // TODO: plug in the config

    DatetimeEmptyBucketGenerator(Expression field, Expression buckets, Expression from, Expression to, FoldContext foldContext) {
        this(foldToLong(foldContext, from), foldToLong(foldContext, to), determineRounding(field, buckets, from, to, foldContext));
    }

    @Override
    public int getEmptyBucketCount() {
        int i = 0;
        for (long bucket = rounding.round(from); bucket < to; bucket = rounding.nextRoundingValue(bucket)) {
            i++;
        }
        return i;
    }

    @Override
    public Block generate(BlockFactory blockFactory, int maxPositionsInBucket) {
        try (LongBlock.Builder newBlockBuilder = (LongBlock.Builder) ElementType.LONG.newBlockBuilder(maxPositionsInBucket, blockFactory)) {
            int i = 0;
            for (long bucket = rounding.round(from); bucket < to; bucket = rounding.nextRoundingValue(bucket)) {
                newBlockBuilder.appendLong(bucket);
                i++;
            }
            while (i < maxPositionsInBucket) {
                newBlockBuilder.appendNull();
                i++;
            }
            return newBlockBuilder.build();
        }
    }

    static Rounding.Prepared determineRounding(
        Expression field,
        Expression buckets,
        Expression from,
        Expression to,
        FoldContext foldContext
    ) {
        assert field.dataType() == DataType.DATETIME || field.dataType() == DataType.DATE_NANOS : "expected date type; got " + field;
        if (buckets.dataType().isWholeNumber()) {
            int b = ((Number) buckets.fold(foldContext)).intValue();
            long f = foldToLong(foldContext, from);
            long t = foldToLong(foldContext, to);
            return new DateRoundingPicker(b, f, t).pickRounding().prepareForUnknown();
        } else {
            assert DataType.isTemporalAmount(buckets.dataType()) : "Unexpected span data type [" + buckets.dataType() + "]";
            return DateTrunc.createRounding(buckets.fold(foldContext), DEFAULT_TZ);
        }
    }

    private static long foldToLong(FoldContext ctx, Expression e) {
        Object value = Foldables.valueOf(ctx, e);
        return DataType.isDateTime(e.dataType()) ? ((Number) value).longValue() : dateTimeToLong(((BytesRef) value).utf8ToString());
    }

    private record DateRoundingPicker(int buckets, long from, long to) {
        Rounding pickRounding() {
            Rounding prev = LARGEST_HUMAN_DATE_ROUNDING;
            for (Rounding r : HUMAN_DATE_ROUNDINGS) {
                if (roundingIsOk(r)) {
                    prev = r;
                } else {
                    return prev;
                }
            }
            return prev;
        }

        /**
         * True if the rounding produces less than or equal to the requested number of buckets.
         */
        boolean roundingIsOk(Rounding rounding) {
            Rounding.Prepared r = rounding.prepareForUnknown();
            long bucket = r.round(from);
            int used = 0;
            while (used < buckets) {
                bucket = r.nextRoundingValue(bucket);
                used++;
                if (bucket >= to) {
                    return true;
                }
            }
            return false;
        }
    }
}
