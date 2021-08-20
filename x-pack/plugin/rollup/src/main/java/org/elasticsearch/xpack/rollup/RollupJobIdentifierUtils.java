/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig.CALENDAR_INTERVAL;
import static org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig.FIXED_INTERVAL;
import static org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig.INTERVAL;

/**
 * This class contains utilities to identify which jobs are the "best" for a given aggregation tree.
 * It allows the caller to pass in a set of possible rollup job capabilities and get in return
 * a smaller (but not guaranteed minimal) set of valid jobs that can be searched.
 */
public class RollupJobIdentifierUtils {

    static final Comparator<RollupJobCaps> COMPARATOR = RollupJobIdentifierUtils.getComparator();

    /**
     * Given the aggregation tree and a list of available job capabilities, this method will return a set
     * of the "best" jobs that should be searched.
     *
     * It does this by recursively descending through the aggregation tree and independently pruning the
     * list of valid job caps in each branch.  When a leaf node is reached in the branch, the remaining
     * jobs are sorted by "best'ness" (see {@link #getComparator()} for the implementation)
     * and added to a global set of "best jobs".
     *
     * Once all branches have been evaluated, the final set is returned to the calling code.
     *
     * Job "best'ness" is, briefly, the job(s) that have
     *  - The larger compatible date interval
     *  - Fewer and larger interval histograms
     *  - Fewer terms groups
     *
     * Note: the final set of "best" jobs is not guaranteed to be minimal, there may be redundant effort
     * due to independent branches choosing jobs that are subsets of other branches.
     *
     * @param source The source aggregation that we are trying to find jobs for
     * @param jobCaps The total set of available job caps on the index/indices
     * @return A set of the "best" jobs out of the total job caps
     */
    public static Set<RollupJobCaps> findBestJobs(AggregationBuilder source, Set<RollupJobCaps> jobCaps) {
        // TODO there is an opportunity to optimize the returned caps to find the minimal set of required caps.
        // For example, one leaf may have equally good jobs [A,B], while another leaf finds only job [B] to be best.
        // If job A is a subset of job B, we could simply search job B in isolation and get the same results
        //
        // We can't do that today, because we don't (yet) have way of determining if one job is a sub/super set of another
        Set<RollupJobCaps> bestCaps = new HashSet<>();
        doFindBestJobs(source, new ArrayList<>(jobCaps), bestCaps);
        return bestCaps;
    }

    private static void doFindBestJobs(AggregationBuilder source, List<RollupJobCaps> jobCaps, Set<RollupJobCaps> bestCaps) {
        if (source.getWriteableName().equals(DateHistogramAggregationBuilder.NAME)) {
            checkDateHisto((DateHistogramAggregationBuilder) source, jobCaps, bestCaps);
        } else if (source.getWriteableName().equals(HistogramAggregationBuilder.NAME)) {
            checkHisto((HistogramAggregationBuilder) source, jobCaps, bestCaps);
        } else if (RollupField.SUPPORTED_METRICS.contains(source.getWriteableName())) {
            checkVSLeaf((ValuesSourceAggregationBuilder.LeafOnly) source, jobCaps, bestCaps);
        } else if (source.getWriteableName().equals(TermsAggregationBuilder.NAME)) {
            checkTerms((TermsAggregationBuilder) source, jobCaps, bestCaps);
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
     * Find the set of date_histo's with the largest granularity interval
     */
    private static void checkDateHisto(DateHistogramAggregationBuilder source, List<RollupJobCaps> jobCaps, Set<RollupJobCaps> bestCaps) {
        ArrayList<RollupJobCaps> localCaps = new ArrayList<>();
        for (RollupJobCaps cap : jobCaps) {
            RollupJobCaps.RollupFieldCaps fieldCaps = cap.getFieldCaps().get(source.field());
            if (fieldCaps != null) {
                for (Map<String, Object> agg : fieldCaps.getAggs()) {
                    if (agg.get(RollupField.AGG).equals(DateHistogramAggregationBuilder.NAME)) {
                        DateHistogramInterval interval = new DateHistogramInterval((String) agg.get(RollupField.INTERVAL));

                        ZoneId thisTimezone = ZoneId.of(((String) agg.get(DateHistogramGroupConfig.TIME_ZONE)), ZoneId.SHORT_IDS);
                        ZoneId sourceTimeZone = source.timeZone() == null
                            ? DateHistogramGroupConfig.DEFAULT_ZONEID_TIMEZONE
                            : ZoneId.of(source.timeZone().toString(), ZoneId.SHORT_IDS);

                        // Ensure we are working on the same timezone
                        if (thisTimezone.getRules().equals(sourceTimeZone.getRules()) == false) {
                            continue;
                        }

                        /*
                          This is convoluted, but new + legacy intervals makes for a big pattern match.
                          We have to match up date_histo [interval, fixed_interval, calendar_interval] with
                          rollup config [interval, fixed_interval, calendar_interval]

                          To keep rightward drift to a minimum we break out of the loop if a successful match is found
                        */

                        DateHistogramInterval configCalendarInterval = agg.get(CALENDAR_INTERVAL) != null
                            ? new DateHistogramInterval((String) agg.get(CALENDAR_INTERVAL))
                            : null;
                        DateHistogramInterval configFixedInterval = agg.get(FIXED_INTERVAL) != null
                            ? new DateHistogramInterval((String) agg.get(FIXED_INTERVAL))
                            : null;
                        DateHistogramInterval configLegacyInterval = agg.get(INTERVAL) != null
                            ? new DateHistogramInterval((String) agg.get(INTERVAL))
                            : null;

                        // If histo used calendar_interval explicitly
                        if (source.getCalendarInterval() != null) {
                            DateHistogramInterval requestInterval = source.getCalendarInterval();

                            // Try to use explicit calendar_interval on config if it exists
                            if (validateCalendarInterval(requestInterval, configCalendarInterval)) {
                                localCaps.add(cap);
                                break;
                            }

                            // Otherwise fall back to old style where we prefer calendar over fixed (e.g. `1h` == calendar)
                            if (validateCalendarInterval(requestInterval, configLegacyInterval)) {
                                localCaps.add(cap);
                                break;
                            }

                            // Note that this ignores FIXED_INTERVAL on purpose, it would not be compatible

                        } else if (source.getFixedInterval() != null) {
                            // If histo used fixed_interval explicitly

                            DateHistogramInterval requestInterval = source.getFixedInterval();

                            // Try to use explicit fixed_interval on config if it exists
                            if (validateFixedInterval(requestInterval, configFixedInterval)) {
                                localCaps.add(cap);
                                break;
                            }

                            // Otherwise fall back to old style
                            if (validateFixedInterval(requestInterval, configLegacyInterval)) {
                                localCaps.add(cap);
                                break;
                            }

                            // Note that this ignores CALENDER_INTERVAL on purpose, it would not be compatible

                        } else if (source.dateHistogramInterval() != null) {
                            // The histo used a deprecated interval method, so meaning is ambiguous.
                            // Use legacy method of preferring calendar over fixed
                            final DateHistogramInterval requestInterval = source.dateHistogramInterval();

                            // Try to use explicit calendar_interval on config if it exists
                            // Both must be calendar intervals
                            if (validateCalendarInterval(requestInterval, configCalendarInterval)) {
                                localCaps.add(cap);
                                break;
                            }

                            // Otherwise fall back to old style where we prefer calendar over fixed (e.g. `1h` == calendar)
                            // Need to verify that the config interval is in fact calendar here
                            if (isCalendarInterval(configLegacyInterval)
                                && validateCalendarInterval(requestInterval, configLegacyInterval)) {

                                localCaps.add(cap);
                                break;
                            }

                            // The histo's interval couldn't be parsed as a calendar, so it is assumed fixed.
                            // Try to use explicit fixed_interval on config if it exists
                            if (validateFixedInterval(requestInterval, configFixedInterval)) {
                                localCaps.add(cap);
                                break;
                            }

                        } else if (source.interval() != 0) {
                            // Otherwise fall back to old style interval millis
                            // Need to verify that the config interval is not calendar here
                            if (isCalendarInterval(configLegacyInterval) == false
                                && validateFixedInterval(new DateHistogramInterval(source.interval() + "ms"), configLegacyInterval)) {

                                localCaps.add(cap);
                                break;
                            }
                        } else {
                            // This _should not_ happen, but if miraculously it does we need to just quit
                            throw new IllegalArgumentException(
                                "An interval of some variety must be configured on " + "the date_histogram aggregation."
                            );
                        }
                        // If we get here nothing matched, and we can break out
                        break;
                    }
                }
            }
        }

        if (localCaps.isEmpty()) {
            throw new IllegalArgumentException(
                "There is not a rollup job that has a ["
                    + source.getWriteableName()
                    + "] agg on field ["
                    + source.field()
                    + "] which also satisfies all requirements of query."
            );
        }

        // We are a leaf, save our best caps
        if (source.getSubAggregations().size() == 0) {
            bestCaps.add(getTopEqualCaps(localCaps));
        } else {
            // otherwise keep working down the tree
            source.getSubAggregations().forEach(sub -> doFindBestJobs(sub, localCaps, bestCaps));
        }
    }

    static String retrieveInterval(Map<String, Object> agg) {
        String interval = (String) agg.get(RollupField.INTERVAL);
        if (interval == null) {
            interval = (String) agg.get(CALENDAR_INTERVAL);
        }
        if (interval == null) {
            interval = (String) agg.get(FIXED_INTERVAL);
        }
        if (interval == null) {
            throw new IllegalStateException("Could not find interval in agg cap: " + agg.toString());
        }
        return interval;
    }

    private static boolean isCalendarInterval(DateHistogramInterval interval) {
        return interval != null && DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(interval.toString());
    }

    static boolean validateCalendarInterval(DateHistogramInterval requestInterval, DateHistogramInterval configInterval) {
        if (requestInterval == null || configInterval == null) {
            return false;
        }

        // The request must be gte the config. The CALENDAR_ORDERING map values are integers representing
        // relative orders between the calendar units
        Rounding.DateTimeUnit requestUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(requestInterval.toString());
        if (requestUnit == null) {
            return false;
        }
        Rounding.DateTimeUnit configUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(configInterval.toString());
        if (configUnit == null) {
            return false;
        }

        long requestOrder = requestUnit.getField().getBaseUnit().getDuration().toMillis();
        long configOrder = configUnit.getField().getBaseUnit().getDuration().toMillis();

        // All calendar units are multiples naturally, so we just care about gte
        return requestOrder >= configOrder;
    }

    static boolean validateFixedInterval(DateHistogramInterval requestInterval, DateHistogramInterval configInterval) {
        if (requestInterval == null || configInterval == null) {
            return false;
        }

        // Both are fixed, good to convert to millis now
        long configIntervalMillis = TimeValue.parseTimeValue(configInterval.toString(), "date_histo.config.interval").getMillis();
        long requestIntervalMillis = TimeValue.parseTimeValue(requestInterval.toString(), "date_histo.request.interval").getMillis();

        // Must be a multiple and gte the config
        return requestIntervalMillis >= configIntervalMillis && requestIntervalMillis % configIntervalMillis == 0;
    }

    /**
     * Find the set of histo's with the largest interval
     */
    private static void checkHisto(HistogramAggregationBuilder source, List<RollupJobCaps> jobCaps, Set<RollupJobCaps> bestCaps) {
        ArrayList<RollupJobCaps> localCaps = new ArrayList<>();
        for (RollupJobCaps cap : jobCaps) {
            RollupJobCaps.RollupFieldCaps fieldCaps = cap.getFieldCaps().get(source.field());
            if (fieldCaps != null) {
                for (Map<String, Object> agg : fieldCaps.getAggs()) {
                    if (agg.get(RollupField.AGG).equals(HistogramAggregationBuilder.NAME)) {
                        long interval = (long) agg.get(RollupField.INTERVAL);
                        // query interval must be gte the configured interval, and a whole multiple
                        if (interval <= source.interval() && source.interval() % interval == 0) {
                            localCaps.add(cap);
                        }
                        break;
                    }
                }
            }
        }

        if (localCaps.isEmpty()) {
            throw new IllegalArgumentException(
                "There is not a rollup job that has a ["
                    + source.getWriteableName()
                    + "] agg on field ["
                    + source.field()
                    + "] which also satisfies all requirements of query."
            );
        }

        // We are a leaf, save our best caps
        if (source.getSubAggregations().size() == 0) {
            bestCaps.add(getTopEqualCaps(localCaps));
        } else {
            // otherwise keep working down the tree
            source.getSubAggregations().forEach(sub -> doFindBestJobs(sub, localCaps, bestCaps));
        }
    }

    /**
     * Ensure that the terms aggregation is supported by one or more job caps.  There is no notion of "best"
     * caps for terms, it is either supported or not.
     */
    private static void checkTerms(TermsAggregationBuilder source, List<RollupJobCaps> jobCaps, Set<RollupJobCaps> bestCaps) {
        ArrayList<RollupJobCaps> localCaps = new ArrayList<>();
        for (RollupJobCaps cap : jobCaps) {
            RollupJobCaps.RollupFieldCaps fieldCaps = cap.getFieldCaps().get(source.field());
            if (fieldCaps != null) {
                for (Map<String, Object> agg : fieldCaps.getAggs()) {
                    if (agg.get(RollupField.AGG).equals(TermsAggregationBuilder.NAME)) {
                        localCaps.add(cap);
                        break;
                    }
                }
            }
        }

        if (localCaps.isEmpty()) {
            throw new IllegalArgumentException(
                "There is not a rollup job that has a ["
                    + source.getWriteableName()
                    + "] agg on field ["
                    + source.field()
                    + "] which also satisfies all requirements of query."
            );
        }

        // We are a leaf, save our best caps
        if (source.getSubAggregations().size() == 0) {
            bestCaps.add(getTopEqualCaps(localCaps));
        } else {
            // otherwise keep working down the tree
            source.getSubAggregations().forEach(sub -> doFindBestJobs(sub, localCaps, bestCaps));
        }
    }

    /**
     * Ensure that the metrics are supported by one or more job caps.  There is no notion of "best"
     * caps for metrics, it is either supported or not.
     */
    private static void checkVSLeaf(
        ValuesSourceAggregationBuilder.LeafOnly<?, ?> source,
        List<RollupJobCaps> jobCaps,
        Set<RollupJobCaps> bestCaps
    ) {
        ArrayList<RollupJobCaps> localCaps = new ArrayList<>();
        for (RollupJobCaps cap : jobCaps) {
            RollupJobCaps.RollupFieldCaps fieldCaps = cap.getFieldCaps().get(source.field());
            if (fieldCaps != null) {
                for (Map<String, Object> agg : fieldCaps.getAggs()) {
                    if (agg.get(RollupField.AGG).equals(source.getWriteableName())) {
                        localCaps.add(cap);
                        break;
                    }
                }
            }
        }

        if (localCaps.isEmpty()) {
            throw new IllegalArgumentException(
                "There is not a rollup job that has a ["
                    + source.getWriteableName()
                    + "] agg with name ["
                    + source.getName()
                    + "] which also satisfies all requirements of query."
            );
        }

        // Metrics are always leaves so go ahead and add to best caps
        bestCaps.add(getTopEqualCaps(localCaps));
    }

    private static RollupJobCaps getTopEqualCaps(List<RollupJobCaps> caps) {
        assert caps.isEmpty() == false;
        caps.sort(COMPARATOR);
        return caps.get(0);
    }

    private static Comparator<RollupJobCaps> getComparator() {
        return (o1, o2) -> {
            if (o1 == null) {
                throw new NullPointerException("RollupJobCap [o1] cannot be null");
            }
            if (o2 == null) {
                throw new NullPointerException("RollupJobCap [o2] cannot be null");
            }

            if (o1.equals(o2)) {
                return 0;
            }

            long thisTime = Long.MAX_VALUE;
            long thatTime = Long.MAX_VALUE;

            // histogram intervals are averaged and compared, with the idea that
            // a larger average == better, because it will generate fewer documents
            float thisHistoWeights = 0;
            float thatHistoWeights = 0;
            long counter = 0;

            // Similarly, fewer terms groups will generate fewer documents, so
            // we count the number of terms groups
            long thisTermsWeights = 0;
            long thatTermsWeights = 0;

            // Iterate over the first Caps and collect the various stats
            for (RollupJobCaps.RollupFieldCaps fieldCaps : o1.getFieldCaps().values()) {
                for (Map<String, Object> agg : fieldCaps.getAggs()) {
                    if (agg.get(RollupField.AGG).equals(DateHistogramAggregationBuilder.NAME)) {
                        thisTime = new DateHistogramInterval(retrieveInterval(agg)).estimateMillis();
                    } else if (agg.get(RollupField.AGG).equals(HistogramAggregationBuilder.NAME)) {
                        thisHistoWeights += (long) agg.get(RollupField.INTERVAL);
                        counter += 1;
                    } else if (agg.get(RollupField.AGG).equals(TermsAggregationBuilder.NAME)) {
                        thisTermsWeights += 1;
                    }
                }
            }
            thisHistoWeights = counter == 0 ? 0 : thisHistoWeights / counter;

            // Iterate over the second Cap and collect the same stats
            counter = 0;
            for (RollupJobCaps.RollupFieldCaps fieldCaps : o2.getFieldCaps().values()) {
                for (Map<String, Object> agg : fieldCaps.getAggs()) {
                    if (agg.get(RollupField.AGG).equals(DateHistogramAggregationBuilder.NAME)) {
                        thatTime = new DateHistogramInterval(retrieveInterval(agg)).estimateMillis();
                    } else if (agg.get(RollupField.AGG).equals(HistogramAggregationBuilder.NAME)) {
                        thatHistoWeights += (long) agg.get(RollupField.INTERVAL);
                        counter += 1;
                    } else if (agg.get(RollupField.AGG).equals(TermsAggregationBuilder.NAME)) {
                        thatTermsWeights += 1;
                    }
                }
            }
            thatHistoWeights = counter == 0 ? 0 : thatHistoWeights / counter;

            // Compare on date interval first
            // The "smaller" job is the one with the larger interval
            int timeCompare = Long.compare(thisTime, thatTime);
            if (timeCompare != 0) {
                return -timeCompare;
            }

            // If dates are the same, the "smaller" job is the one with a larger histo avg histo weight.
            // Not bullet proof, but heuristically we prefer:
            // - one job with interval 100 (avg 100) over one job with interval 10 (avg 10)
            // - one job with interval 100 (avg 100) over one job with ten histos @ interval 10 (avg 10)
            // because in both cases the larger intervals likely generate fewer documents
            //
            // The exception is if one of jobs had no histo (avg 0) then we prefer that
            int histoCompare = Float.compare(thisHistoWeights, thatHistoWeights);
            if (histoCompare != 0) {
                if (thisHistoWeights == 0) {
                    return -1;
                } else if (thatHistoWeights == 0) {
                    return 1;
                }
                return -histoCompare;
            }

            // If dates and histo are same, the "smaller" job is the one with fewer terms aggs since
            // hopefully will generate fewer docs
            return Long.compare(thisTermsWeights, thatTermsWeights);

            // Ignoring metrics for now, since the "best job" resolution doesn't take those into account
            // and we rely on the msearch functionality to merge away and duplicates
            // Could potentially optimize there in the future to choose jobs with more metric
            // coverage
        };
    }
}
