/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * This class provides a number of static utilities that help convert a non-rollup
 * aggregation into an aggregation that conforms to the rollup conventions
 *
 * Documents are expected to follow a convention that looks like:
 * <pre>{@code
 * {
 *   "_rollup.version": 1,
 *   "ts.date_histogram.timestamp": 1494796800000,
 *   "ts.date_histogram._count": 1000000000,
 *   "ts.date_histogram.interval": 86400000,
 *   "foo.max.value": 19995
 *   "title.terms.value": "abc"
 * }
 * }</pre>
 *
 *
 * The only publicly "consumable" API is {@link #translateAggregation(AggregationBuilder, NamedWriteableRegistry)}.
 */
public class RollupRequestTranslator {

    /**
     * Translates a non-rollup aggregation tree into a rollup-enabled agg tree.  For example, the
     * source aggregation may look like this:
     *
     * <pre>{@code
     * POST /foo/_rollup_search
     * {
     *   "aggregations": {
     *     "the_histo": {
     *       "date_histogram" : {
     *         "field" : "ts",
     *         "calendar_interval" : "1d"
     *       },
     *       "aggs": {
     *         "the_max": {
     *           "max": {
     *             "field": "foo"
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * Which is then translated into an aggregation looking like this:
     *
     * <pre>{@code
     * POST /rolled_foo/_search
     * {
     *   "aggregations" : {
     *     "filter_histo" : {
     *       "filter" : {
     *         "bool" : {
     *           "must" : [
     *             { "term" : { "_rollup.version" : 1   } },
     *             { "term": {  "ts.date_histogram.interval" : "1d"  } }
     *           ]
     *         }
     *       },
     *       "aggregations" : {
     *         "the_histo" : {
     *           "date_histogram" : {
     *             "field" : "ts.date_histogram.timestamp",
     *             "calendar_interval" : "1d"
     *           },
     *           "aggregations" : {
     *             "the_histo._count" : {
     *               "sum" : { "field" : "ts.date_histogram._count" }
     *             },
     *             "the_max" : {
     *               "max" : { "field" : "foo.max.value" }
     *             }
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * The various conventions that are applied during the translation are elucidated in the comments of the
     * relevant method below.
     *
     * @param source           The source aggregation to translate into rollup-enabled version
     * @param registry  Registry containing the various aggregations so that we can easily
     *                  deserialize into a stream for cloning
     * @return  Returns the fully translated aggregation tree. Note that it returns a list instead
     *          of a single AggBuilder, since some aggregations (e.g. avg) may result in two
     *          translated aggs (sum + count)
     */
    public static List<AggregationBuilder> translateAggregation(AggregationBuilder source, NamedWriteableRegistry registry) {

        if (source.getWriteableName().equals(DateHistogramAggregationBuilder.NAME)) {
            return translateDateHistogram((DateHistogramAggregationBuilder) source, registry);
        } else if (source.getWriteableName().equals(HistogramAggregationBuilder.NAME)) {
            return translateHistogram((HistogramAggregationBuilder) source, registry);
        } else if (RollupField.SUPPORTED_METRICS.contains(source.getWriteableName())) {
            return translateVSLeaf((ValuesSourceAggregationBuilder.LeafOnly) source, registry);
        } else if (source.getWriteableName().equals(TermsAggregationBuilder.NAME)) {
            return translateTerms((TermsAggregationBuilder) source, registry);
        } else {
            throw new IllegalArgumentException(
                "Unable to translate aggregation tree into Rollup.  Aggregation ["
                    + source.getName()
                    + "] is of type ["
                    + source.getClass().getSimpleName()
                    + "] which is "
                    + "currently unsupported."
            );
        }
    }

    /**
     * Translate a normal date_histogram into one that follows the rollup conventions.
     * Notably, it adds a Sum metric to calculate the doc_count in each bucket.
     *
     * E.g. this date_histogram:
     *
     * <pre>{@code
     * POST /foo/_rollup_search
     * {
     *   "aggregations": {
     *     "the_histo": {
     *       "date_histogram" : {
     *         "field" : "ts",
     *         "calendar_interval" : "day"
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * is translated into:
     *
     * <pre>{@code
     * POST /rolled_foo/_search
     * {
     *   "aggregations" : {
     *     "the_histo" : {
     *       "date_histogram" : {
     *         "field" : "ts.date_histogram.timestamp",
     *         "interval" : "day"
     *       },
     *       "aggregations" : {
     *         "the_histo._count" : {
     *           "sum" : { "field" : "ts.date_histogram._count" }
     *         }
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * The conventions are:
     * <ul>
     *     <li>Named: same as the source histogram</li>
     *     <li>Field: `{timestamp field}.date_histogram.timestamp`</li>
     *     <li>Add a SumAggregation to each bucket:</li>
     *     <li>
     *         <ul>
     *             <li>Named: `{parent histogram name}._count`</li>
     *             <li>Field: `{timestamp field}.date_histogram._count`</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     */
    private static List<AggregationBuilder> translateDateHistogram(
        DateHistogramAggregationBuilder source,
        NamedWriteableRegistry registry
    ) {

        return translateVSAggBuilder(source, registry, () -> {
            DateHistogramAggregationBuilder rolledDateHisto = new DateHistogramAggregationBuilder(source.getName());

            if (source.getCalendarInterval() != null) {
                rolledDateHisto.calendarInterval(source.getCalendarInterval());
            } else if (source.getFixedInterval() != null) {
                rolledDateHisto.fixedInterval(source.getFixedInterval());
            }

            ZoneId timeZone = source.timeZone() == null ? DateHistogramGroupConfig.DEFAULT_ZONEID_TIMEZONE : source.timeZone();
            rolledDateHisto.timeZone(timeZone);

            rolledDateHisto.offset(source.offset());
            if (source.extendedBounds() != null) {
                rolledDateHisto.extendedBounds(source.extendedBounds());
            }
            if (Strings.isNullOrEmpty(source.format()) == false) {
                rolledDateHisto.format(source.format());
            }
            rolledDateHisto.keyed(source.keyed());
            rolledDateHisto.minDocCount(source.minDocCount());
            rolledDateHisto.order(source.order());
            rolledDateHisto.field(RollupField.formatFieldName(source, RollupField.TIMESTAMP));
            rolledDateHisto.setMetadata(source.getMetadata());
            return rolledDateHisto;
        });
    }

    /**
     * Translate a normal histogram into one that follows the rollup conventions.
     * Notably, it adds a Sum metric to calculate the doc_count in each bucket.
     *
     * Conventions are identical to a date_histogram (excepting date-specific details), so see
     * {@link #translateDateHistogram(DateHistogramAggregationBuilder, NamedWriteableRegistry)} for
     * a complete list of conventions, examples, etc
     */
    private static List<AggregationBuilder> translateHistogram(HistogramAggregationBuilder source, NamedWriteableRegistry registry) {

        return translateVSAggBuilder(source, registry, () -> {
            HistogramAggregationBuilder rolledHisto = new HistogramAggregationBuilder(source.getName());

            rolledHisto.interval(source.interval());
            rolledHisto.offset(source.offset());
            if (Double.isFinite(source.minBound()) && Double.isFinite(source.maxBound())) {
                rolledHisto.extendedBounds(source.minBound(), source.maxBound());
            }
            rolledHisto.keyed(source.keyed());
            rolledHisto.minDocCount(source.minDocCount());
            rolledHisto.order(source.order());
            rolledHisto.field(RollupField.formatFieldName(source, RollupField.VALUE));
            rolledHisto.setMetadata(source.getMetadata());
            return rolledHisto;
        });
    }

    /**
     * Translate a normal terms agg into one that follows the rollup conventions.
     * Notably, it adds metadata to the terms, and a Sum metric to calculate the doc_count
     * in each bucket.
     *
     * E.g. this terms agg:
     *
     * <pre>{@code
     * POST /foo/_rollup_search
     * {
     *   "aggregations": {
     *     "the_terms": {
     *       "terms" : {
     *         "field" : "foo"
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * is translated into:
     *
     * <pre>{@code
     * POST /rolled_foo/_search
     * {
     *   "aggregations" : {
     *     "the_terms" : {
     *       "terms" : {
     *         "field" : "foo.terms.value"
     *       },
     *       "aggregations" : {
     *         "the_terms._count" : {
     *           "sum" : { "field" : "foo.terms._count" }
     *         }
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * The conventions are:
     * <ul>
     *     <li>Named: same as the source terms agg</li>
     *     <li>Field: `{field name}.terms.value`</li>
     *     <li>Add a SumAggregation to each bucket:</li>
     *     <li>
     *         <ul>
     *             <li>Named: `{parent terms name}._count`</li>
     *             <li>Field: `{field name}.terms._count`</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     */
    private static List<AggregationBuilder> translateTerms(TermsAggregationBuilder source, NamedWriteableRegistry registry) {

        return translateVSAggBuilder(source, registry, () -> {
            TermsAggregationBuilder rolledTerms = new TermsAggregationBuilder(source.getName());
            if (source.userValueTypeHint() != null) {
                rolledTerms.userValueTypeHint(source.userValueTypeHint());
            }
            rolledTerms.field(RollupField.formatFieldName(source, RollupField.VALUE));
            rolledTerms.includeExclude(source.includeExclude());
            if (source.collectMode() != null) {
                rolledTerms.collectMode(source.collectMode());
            }
            rolledTerms.minDocCount(source.minDocCount());
            rolledTerms.executionHint(source.executionHint());
            if (source.order() != null) {
                rolledTerms.order(source.order());
            }
            rolledTerms.shardMinDocCount(source.shardMinDocCount());
            if (source.shardSize() > 0) {
                rolledTerms.shardSize(source.shardSize());
            }
            rolledTerms.showTermDocCountError(source.showTermDocCountError());
            rolledTerms.size(source.size());
            return rolledTerms;
        });
    }

    /**
     * The generic method that does most of the actual heavy-lifting when translating a multi-bucket
     * ValueSourceBuilder.  This method is called by all the agg-specific methods (e.g. translateDateHistogram())
     *
     * @param source The source aggregation that we wish to translate
     * @param registry Named registry for serializing leaf metrics.  Not actually used by this method,
     *                 but is passed downwards for leaf usage
     * @param factory A factory closure that generates a new shallow clone of the `source`. E.g. if `source` is
     *                a date_histogram, the factory will take return a new DateHistogramAggBUilder with matching
     *                parameters.  It is not a deep clone however; the returned object won't have children
     *                set.
     * @param <T> The type of ValueSourceAggBuilder that we are working with
     * @return the translated multi-bucket ValueSourceAggBuilder
     */
    private static <T extends ValuesSourceAggregationBuilder<T>> List<AggregationBuilder> translateVSAggBuilder(
        T source,
        NamedWriteableRegistry registry,
        Supplier<T> factory
    ) {

        T rolled = factory.get();

        // Translate all subaggs and add to the newly translated agg
        // NOTE: using for loop instead of stream because compiler explodes with a bug :/
        for (AggregationBuilder subAgg : source.getSubAggregations()) {
            List<AggregationBuilder> translated = translateAggregation(subAgg, registry);
            for (AggregationBuilder t : translated) {
                rolled.subAggregation(t);
            }
        }

        // Count is derived from a sum, e.g.
        // "my_date_histo._count": { "sum": { "field": "foo.date_histogram._count" } } }
        rolled.subAggregation(
            new SumAggregationBuilder(RollupField.formatCountAggName(source.getName())).field(
                RollupField.formatFieldName(source, RollupField.COUNT_FIELD)
            )
        );

        return Collections.singletonList(rolled);
    }

    /**
     * Translates leaf aggs (min/max/sum/etc) into their rollup version.  For simple aggs like `min`,
     * this is nearly a 1:1 copy.  The source is deserialized into a new object, and the field is adjusted
     * according to convention.  E.g. for a `min` agg:
     *
     * <pre>{@code
     * {
     *   "the_min":{ "min" : { "field" : "some_field" } }
     * }
     * }</pre>
     *
     * the translation would be:
     *
     * <pre>{@code
     * {
     *   "the_min":{ "min" : { "field" : "some_field.min.value" }}
     * }
     * }</pre>
     *
     * However, for `avg` metrics (and potentially others in the future), the agg is translated into
     * a sum + sum aggs; one for count and one for sum.  When unrolling these will be combined back into
     * a single avg.  Note that we also have to rename the avg agg name to distinguish it from empty
     * buckets.  E.g. for an `avg` agg:
     *
     * <pre>{@code
     * {
     *   "the_avg":{ "avg" : { "field" : "some_field" }}
     * }
     * }</pre>
     *
     * the translation would be:
     *
     * <pre>{@code
     * [
     *   {
     *    "the_avg.value": {
     *      "sum" : { "field" : "some_field.avg.value" }}
     *   },
     *   {
     *    "the_avg._count": { "sum" : { "field" : "some_field.avg._count" }}
     *   }
     * ]
     * }</pre>
     *
     * The conventions are:
     * <ul>
     *     <li>Agg type: same as source agg</li>
     *     <li>Named: same as the source agg</li>
     *     <li>Field: `{agg_type}.{field_name}.value`</li>
     * </ul>
     *
     * IF the agg is an AvgAgg, the following additional conventions are added:
     * <ul>
     *     <li>Agg type: becomes SumAgg, instead of AvgAgg</li>
     *     <li>Named: {source name}.value</li>
     *     <li>Additionally, an extra SumAgg is added:</li>
     *     <li>
     *         <ul>
     *             <li>Named: `{source name}._count`</li>
     *             <li>Field: `{field name}.{agg type}._count`</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     *
     * @param metric  The source leaf aggregation we wish to translate
     * @param registry A registry of NamedWriteable's so we can do a simple serialize/deserialize for
     *                 most of the leafs to easily clone them
     * @return The translated leaf aggregation
     */
    private static List<AggregationBuilder> translateVSLeaf(
        ValuesSourceAggregationBuilder.LeafOnly<?, ?> metric,
        NamedWriteableRegistry registry
    ) {

        List<AggregationBuilder> rolledMetrics;

        // If it's an avg, we have to manually convert it into sum + sum aggs
        if (metric instanceof AvgAggregationBuilder) {
            rolledMetrics = new ArrayList<>(2);

            // Avg metric is translated into a SumAgg, e.g.
            // Note: we change the agg name to prevent conflicts with empty buckets
            // "the_avg.value" : { "field" : "some_field.avg.value" }}
            SumAggregationBuilder value = new SumAggregationBuilder(RollupField.formatValueAggName(metric.getName()));
            value.field(RollupField.formatFieldName(metric, RollupField.VALUE));
            rolledMetrics.add(value);

            // Count is derived from a sum, e.g.
            // "the_avg._count": { "sum" : { "field" : "some_field.avg._count" }}
            rolledMetrics.add(
                new SumAggregationBuilder(RollupField.formatCountAggName(metric.getName())).field(
                    RollupField.formatFieldName(metric, RollupField.COUNT_FIELD)
                )
            );

            return rolledMetrics;
        }

        // Otherwise, we can cheat and serialize/deserialze into a temp stream as an easy way to clone
        // leaf metrics, since they don't have any sub-aggs
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try {
                output.writeString(metric.getType());
                metric.writeTo(output);
                try (
                    StreamInput stream = output.bytes().streamInput();
                    NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(stream, registry)
                ) {

                    ValuesSourceAggregationBuilder<?> serialized = ((ValuesSourceAggregationBuilder) in.readNamedWriteable(
                        AggregationBuilder.class
                    )).field(RollupField.formatFieldName(metric, RollupField.VALUE));

                    return Collections.singletonList(serialized);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
