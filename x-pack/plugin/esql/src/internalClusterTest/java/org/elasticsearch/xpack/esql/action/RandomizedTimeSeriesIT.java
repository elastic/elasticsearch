/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.aggregation.Temporality;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@SuppressWarnings("unchecked")
@ESIntegTestCase.ClusterScope(maxNumDataNodes = 1)
public class RandomizedTimeSeriesIT extends AbstractEsqlIntegTestCase {
    private static final Long NUM_DOCS = 2000L;
    private static final Long TIME_RANGE_SECONDS = 3600L;
    private static final String DATASTREAM_NAME = "tsit_ds";
    private static final Integer SECONDS_IN_WINDOW = 60;

    record WindowOption(String label, int seconds) {}

    /** A timestamp-value pair used for boundary interpolation calculations. */
    record TimestampedValue(Instant timestamp, double value) {}

    private static final List<WindowOption> WINDOW_OPTIONS = List.of(
        new WindowOption("10 seconds", 10),
        new WindowOption("30 seconds", 30),
        new WindowOption("1 minute", 60),
        new WindowOption("2 minutes", 120),
        new WindowOption("3 minutes", 180),
        new WindowOption("5 minutes", 300),
        new WindowOption("10 minutes", 600),
        new WindowOption("30 minutes", 1800),
        new WindowOption("1 hour", 3600)
    );
    private static final Map<DeltaAgg, String> DELTA_AGG_METRIC_MAP = Map.of(
        DeltaAgg.RATE,
        "counterl_hdd.bytes.read",
        DeltaAgg.IRATE,
        "counterl_hdd.bytes.read",
        DeltaAgg.IDELTA,
        "gaugel_hdd.bytes.used",
        DeltaAgg.INCREASE,
        "counterl_hdd.bytes.read",
        DeltaAgg.DELTA,
        "gaugel_hdd.bytes.used"
    );

    private List<XContentBuilder> documents;
    private TSDataGenerationHelper dataGenerationHelper;

    /**
     * Materializes all rows from a query response.
     *
     * @return list of rows, where each row is a list of column values in query output order
     */
    List<List<Object>> consumeRows(EsqlQueryResponse resp) {
        List<List<Object>> rows = new ArrayList<>();
        resp.rows().forEach(rowIter -> {
            List<Object> row = new ArrayList<>();
            rowIter.forEach(row::add);
            rows.add(row);
        });
        return rows;
    }

    /**
     * Groups documents by their dimension values and time bucket.
     *
     * @param docs the raw indexed documents
     * @param groupingAttributes dimension attribute names to group by
     * @param secondsInWindow time bucket width in seconds
     * @return map keyed by a composite key (list of {@code "attr:value"} pairs followed by window-start
     *         epoch-seconds) to the list of raw document maps in that group
     */
    Map<List<String>, List<Map<String, Object>>> groupedRows(
        List<XContentBuilder> docs,
        List<String> groupingAttributes,
        int secondsInWindow
    ) {
        Map<List<String>, List<Map<String, Object>>> groupedMap = new HashMap<>();
        for (XContentBuilder doc : docs) {
            Map<String, Object> docMap = XContentHelper.convertToMap(BytesReference.bytes(doc), false, XContentType.JSON).v2();
            @SuppressWarnings("unchecked")
            Map<String, Object> attributes = (Map<String, Object>) docMap.getOrDefault("attributes", Map.of());
            List<String> groupingPairs = groupingAttributes.stream().map(attr -> {
                String value = attributes.getOrDefault(attr, "").toString();
                return value.isEmpty() ? null : attr + ":" + value;
            }).filter(Objects::nonNull).toList();
            long timeBucketStart = windowStart(docMap.get("@timestamp"), secondsInWindow);
            var keyList = new ArrayList<>(groupingPairs);
            keyList.add(Long.toString(timeBucketStart));
            groupedMap.computeIfAbsent(keyList, k -> new ArrayList<>()).add(docMap);
        }
        return groupedMap;
    }

    static Long windowStart(Object timestampCell, int secondsInWindow) {
        // This calculation looks a little weird, but it simply performs an integer division that
        // throws away the remainder of the division by secondsInWindow. It rounds down
        // the timestamp to the nearest multiple of secondsInWindow.
        var timestampSeconds = Instant.parse((String) timestampCell).toEpochMilli() / 1000;
        return (timestampSeconds / secondsInWindow) * secondsInWindow;
    }

    enum Agg {
        MAX,
        MIN,
        AVG,
        SUM,
        COUNT
    }

    /**
     * Extracts the integer metric values for {@code metricName} from all documents in the group,
     * filtering out documents that don't have the metric.
     *
     * @param pointsInGroup raw document maps belonging to one time-window/dimension group
     * @return list of metric values for the given metric name
     */
    static List<Integer> valuesInWindow(List<Map<String, Object>> pointsInGroup, String metricName) {
        @SuppressWarnings("unchecked")
        var values = pointsInGroup.stream()
            .map(doc -> ((Map<String, Integer>) doc.get("metrics")).get(metricName))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return values;
    }

    /**
     * Groups documents from a single time window by their timeseries identity.
     *
     * @param pointsInGroup raw document maps belonging to one time-window/dimension group
     * @param metricName the metric to extract values for
     * @return map keyed by timeseries identifier (comma-separated {@code "attr:value"} pairs from document
     *         attributes) to the list of timestamped metric values for that timeseries
     */
    static Map<String, List<TimestampedValue>> groupByTimeseries(List<Map<String, Object>> pointsInGroup, String metricName) {
        return pointsInGroup.stream()
            .filter(doc -> doc.containsKey("metrics") && ((Map<String, Object>) doc.get("metrics")).containsKey(metricName))
            .collect(Collectors.groupingBy(doc -> {
                return ((Map<String, Object>) doc.get("attributes")).entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(","));
            }, Collectors.mapping(doc -> {
                var docTs = Instant.parse((String) doc.get("@timestamp"));
                @SuppressWarnings("unchecked")
                var metricValue = ((Map<String, Object>) doc.get("metrics")).get(metricName);
                var docValue = switch (metricValue) {
                    case Integer i -> i.doubleValue();
                    case Long l -> l.doubleValue();
                    case Float f -> f.doubleValue();
                    case Double d -> d;
                    default -> throw new IllegalStateException(
                        "Unexpected value type: " + metricValue + " of class " + metricValue.getClass()
                    );
                };
                return new TimestampedValue(docTs, docValue);
            }, Collectors.toList())));
    }

    private static Temporality getCounterTemporality(String timeseriesId) {
        String deltaLiteral = Temporality.DELTA.bytesRef().utf8ToString();
        if (timeseriesId.contains(TSDataGenerationHelper.TEMPORALITY_ATTRIBUTE_NAME + ":" + deltaLiteral)) {
            return Temporality.DELTA;
        }
        return Temporality.CUMULATIVE;
    }

    /**
     * Two-level aggregation: first applies {@code timeseriesAgg} within each timeseries, then applies
     * {@code crossAgg} across all per-timeseries results.
     *
     * @param timeseries map keyed by timeseries identifier to data points in one time window
     * @param crossAgg aggregation to apply across timeseries results
     * @param timeseriesAgg aggregation to apply within each timeseries
     */
    static Object aggregatePerTimeseries(Map<String, List<TimestampedValue>> timeseries, Agg crossAgg, Agg timeseriesAgg) {
        var res = timeseries.values().stream().map(timeseriesList -> {
            List<Double> values = timeseriesList.stream().map(tv -> tv.value()).collect(Collectors.toList());
            return aggregateValuesInWindow(values, timeseriesAgg);
        }).filter(Objects::nonNull).toList();

        if (res.isEmpty() && timeseriesAgg == Agg.COUNT) {
            res = List.of(0.0);
        }

        return switch (crossAgg) {
            case MAX -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
            case MIN -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
            case AVG -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            case SUM -> res.isEmpty() ? null : res.stream().mapToDouble(Double::doubleValue).sum();
            case COUNT -> Integer.toUnsignedLong(res.size());
        };
    }

    static Double aggregateValuesInWindow(List<Double> values, Agg agg) {
        return switch (agg) {
            case MAX -> values.stream().max(Double::compareTo).orElseThrow();
            case MIN -> values.stream().min(Double::compareTo).orElseThrow();
            case AVG -> values.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            case SUM -> values.isEmpty() ? null : values.stream().mapToDouble(Double::doubleValue).sum();
            case COUNT -> (double) values.size();
        };
    }

    /**
     * Builds a composite key from a result row that matches the key format used by {@link #groupedRows}.
     *
     * @param row a single result row (list of column values)
     * @param groupingAttributes the dimension attribute names used in the GROUP BY clause
     * @param timestampIndex column index of the time bucket in the row
     * @return list of {@code "attr:value"} pairs followed by the window-start epoch-seconds
     */
    static List<String> getRowKey(List<Object> row, List<String> groupingAttributes, int timestampIndex) {
        List<String> rowKey = new ArrayList<>();
        for (int i = 0; i < groupingAttributes.size(); i++) {
            Object value = row.get(i + timestampIndex + 1);
            if (value != null) {
                rowKey.add(groupingAttributes.get(i) + ":" + value);
            }
        }
        rowKey.add(Long.toString(Instant.parse((String) row.get(timestampIndex)).toEpochMilli() / 1000));
        return rowKey;
    }

    /**
     * Extracts the timeseries identity from a row key by concatenating all elements except the
     * trailing timestamp.
     *
     * @param rowKey composite key as produced by {@link #getRowKey}
     */
    private static String getTimeseriesId(List<String> rowKey) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowKey.size() - 1; i++) {  // Skip the timestamp.
            sb.append(rowKey.get(i));
        }
        return sb.toString();
    }

    static Integer getTimestampIndex(String esqlQuery) {
        // first we get the stats command after the pipe
        var statsIndex = esqlQuery.indexOf("| STATS");
        var nextPipe = esqlQuery.indexOf("|", statsIndex + 1);

        var statsCommand = esqlQuery.substring(statsIndex, nextPipe);
        // then we count the number of commas before "BY "
        var byTbucketIndex = statsCommand.indexOf("BY ");
        var statsPart = statsCommand.substring(0, byTbucketIndex);
        // the number of columns is the number of commas + 1
        return (int) statsPart.chars().filter(ch -> ch == ',').count() + 1;
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("time series available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DataStreamsPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            AggregateMetricMapperPlugin.class,
            TestEncryptionServicePlugin.class,
            EsqlPluginWithEnterpriseOrTrialLicense.class
        );
    }

    record RateRange(Double lower, Double upper) implements Comparable<RateRange> {
        @Override
        public int compareTo(RateRange o) {
            // Compare first by lower bound, then by upper bound
            int cmp = this.lower.compareTo(o.lower);
            if (cmp == 0) {
                return this.upper.compareTo(o.upper);
            }
            return cmp;
        }

        public int compareToFindingMax(RateRange o) {
            // Compare first by upper bound, then by lower bound
            int cmp = this.upper.compareTo(o.upper);
            if (cmp == 0) {
                return this.lower.compareTo(o.lower);
            }
            return cmp;
        }
    }

    enum DeltaAgg {
        RATE("rate"),
        IRATE("irate"),
        IDELTA("idelta"),
        INCREASE("increase"),
        DELTA("delta");

        private final String functionName;

        DeltaAgg(String functionName) {
            this.functionName = functionName;
        }

        String functionName() {
            return functionName;
        }
    }

    /** Aggregated rate statistics (count, max, avg, min, sum) computed across timeseries in a window. */
    record RateStats(Long count, RateRange max, RateRange avg, RateRange min, RateRange sum) {}

    @Nullable
    private static TimestampedValue findLastPoint(List<Map<String, List<TimestampedValue>>> allWindows, int idx, String tsId) {
        if (idx < 0 || idx >= allWindows.size()) return null;
        var pts = allWindows.get(idx).get(tsId);
        if (pts == null) return null;
        return pts.stream().max(Comparator.comparing(TimestampedValue::timestamp)).orElse(null);
    }

    @Nullable
    private static TimestampedValue findFirstPoint(List<Map<String, List<TimestampedValue>>> allWindows, int idx, String tsId) {
        if (idx < 0 || idx >= allWindows.size()) return null;
        var pts = allWindows.get(idx).get(tsId);
        if (pts == null) return null;
        return pts.stream().min(Comparator.comparing(TimestampedValue::timestamp)).orElse(null);
    }

    /**
     * Computes a reference rate or increase for a single timeseries within a time window, matching
     * {@code RateDoubleGroupingAggregatorFunction#computeRate}.
     */
    static Double computeReferenceRateOrIncrease(
        @Nullable TimestampedValue lastInPrevWindow,
        List<TimestampedValue> currentWindow,
        @Nullable TimestampedValue firstInNextWindow,
        Temporality temporality,
        int secondsInWindow,
        boolean isRate
    ) {
        long millisInWindow = secondsInWindow * 1000L;
        if (currentWindow.isEmpty()) {
            return null;
        }
        TimestampedValue firstInWindow = currentWindow.getFirst();
        TimestampedValue lastInWindow = currentWindow.getLast();

        long startTs = firstInWindow.timestamp().toEpochMilli();
        long endTs = lastInWindow.timestamp().toEpochMilli();

        long timebucketStart = startTs / millisInWindow * millisInWindow;
        long timebucketEnd = timebucketStart + millisInWindow;

        double totalIncrease = computeCounterIncreaseBetween(currentWindow, temporality);

        if (lastInPrevWindow != null) {
            totalIncrease += getInterpolatedIncreaseBetween(lastInPrevWindow, firstInWindow, timebucketStart, temporality, true);
            startTs = timebucketStart;
        } else if (currentWindow.size() > 1) {
            totalIncrease += getExtrapolatedIncreaseAtBorder(currentWindow, temporality, secondsInWindow, true);
            startTs = timebucketStart;
        }
        if (firstInNextWindow != null) {
            totalIncrease += getInterpolatedIncreaseBetween(lastInWindow, firstInNextWindow, timebucketEnd, temporality, false);
            endTs = timebucketEnd;
        } else if (currentWindow.size() > 1) {
            totalIncrease += getExtrapolatedIncreaseAtBorder(currentWindow, temporality, secondsInWindow, false);
            endTs = timebucketEnd;
        }

        if (startTs == endTs) {
            if (lastInPrevWindow != null) {
                if (isRate) {
                    // special case: The only value falls exactly on the start border
                    // our rate implementation in this case returns the rate between the point in this window and the point in the last
                    // window
                    List<TimestampedValue> values = List.of(lastInPrevWindow, firstInWindow);
                    double rangeSeconds = (firstInWindow.timestamp().toEpochMilli() - lastInPrevWindow.timestamp().toEpochMilli()) / 1000.0;
                    return computeCounterIncreaseBetween(values, temporality) / rangeSeconds;
                } else {
                    return 0.0;
                }
            }
            return null;
        }
        return isRate ? totalIncrease / ((endTs - startTs) / 1000.0) : totalIncrease;
    }

    private static double computeCounterIncreaseBetween(List<TimestampedValue> currentWindow, Temporality temporality) {
        double increaseInWindow = 0.0;

        if (temporality == Temporality.DELTA) {
            // Intentionally skip the first value: It's increase belongs to
            // the timespan before the timestamp of the first value!
            for (int i = 1; i < currentWindow.size(); i++) {
                increaseInWindow += currentWindow.get(i).value();
            }
        } else {
            if (currentWindow.isEmpty()) {
                return 0;
            }
            double resets = 0;
            for (int i = 1; i < currentWindow.size(); i++) {
                double prevValue = currentWindow.get(i - 1).value();
                double currValue = currentWindow.get(i).value();
                if (currValue < prevValue) {
                    resets += prevValue;
                }
            }
            increaseInWindow = currentWindow.getLast().value + resets - currentWindow.getFirst().value();
        }
        return increaseInWindow;
    }

    static double getInterpolatedIncreaseBetween(
        TimestampedValue left,
        TimestampedValue right,
        long targetTimestamp,
        Temporality temporality,
        boolean isLowerBound
    ) {
        long leftTs = left.timestamp().toEpochMilli();
        long rightTs = right.timestamp().toEpochMilli();
        assert targetTimestamp >= leftTs : "Target timestamp must be greater than or equal to left timestamp";
        assert targetTimestamp <= rightTs : "Target timestamp must be less than or equal to right timestamp";
        assert leftTs != rightTs : "Left and right timestamps must be different for interpolation";

        long timespan = rightTs - leftTs;
        double interpolationWeight = (targetTimestamp - leftTs) * 1.0 / timespan;
        if (isLowerBound) {
            interpolationWeight = 1.0 - interpolationWeight;
        }

        double totalIncrease;
        if (temporality == Temporality.DELTA) {
            // For delta, only the counter value of the right point matters. It represents the total increase between the two points
            totalIncrease = right.value();
        } else {
            if (right.value() >= left.value()) {
                // no reset
                totalIncrease = right.value() - left.value();
            } else {
                // reset, absolute value of right point matters
                totalIncrease = right.value();
            }
        }
        return totalIncrease * interpolationWeight;
    }

    private static double getExtrapolatedIncreaseAtBorder(
        List<TimestampedValue> values,
        Temporality temporality,
        long secondsInWindow,
        boolean isLowerBoundary
    ) {
        assert values.size() >= 2 : "At least two points are required for extrapolation";

        double increase = computeCounterIncreaseBetween(values, temporality);
        long firstTs = values.getFirst().timestamp().toEpochMilli();
        long lastTs = values.getLast().timestamp().toEpochMilli();

        final long sampleTs = lastTs - firstTs;
        final double averageSampleInterval = sampleTs * 1.0 / values.size();
        final double slope = increase / sampleTs;

        assert firstTs != lastTs;

        long millisInWindow = secondsInWindow * 1000L;
        long tbucketStart = firstTs / millisInWindow * millisInWindow;
        long tbucketEnd = tbucketStart + millisInWindow;

        double gap;
        if (isLowerBoundary) {
            gap = firstTs - tbucketStart;
        } else {
            gap = tbucketEnd - lastTs;
        }
        if (gap > 0) {
            if (gap > averageSampleInterval * 1.1) {
                gap = averageSampleInterval / 2.0;
            }
            // the extrapolated increase cannot exceed the first counter value for the lower boundary
            double extrapolatedIncrease = gap * slope;
            if (isLowerBoundary) {
                extrapolatedIncrease = Math.min(extrapolatedIncrease, values.getFirst().value());
            }
            return extrapolatedIncrease;
        }
        return 0.0;
    }

    /**
     * Computes a reference irate matching {@code IrateAggregator#evaluateFinal}.
     * Uses only the last two samples; no adjacent-window data.
     */
    static Double computeReferenceIrate(List<TimestampedValue> currentWindow, Temporality temporality) {
        if (currentWindow == null || currentWindow.size() < 2) {
            return null;
        }
        var last = currentWindow.getLast();
        var secondLast = currentWindow.get(currentWindow.size() - 2);
        double ydiff;
        if (temporality != Temporality.DELTA && last.value() >= secondLast.value()) {
            ydiff = last.value() - secondLast.value();
        } else {
            ydiff = last.value();
        }
        long xdiff = last.timestamp().toEpochMilli() - secondLast.timestamp().toEpochMilli();
        return ydiff / xdiff * 1000.0;
    }

    /**
     * Computes a reference delta range for a single timeseries in a time window.
     * The raw delta is {@code lastValue - firstValue}. The ES DeltaAggregator applies PromQL-style
     * extrapolation that scales the delta up to cover the full bucket width, so the actual result
     * falls between the raw delta and {@code delta * windowSizeFactor}.
     *
     * @return a {@code [lower, upper]} range, or {@code null} if fewer than 2 points
     */
    static double[] computeReferenceDelta(List<TimestampedValue> currentWindow, int secondsInWindow) {
        if (currentWindow == null || currentWindow.size() < 2) {
            return null;
        }
        double firstValue = currentWindow.getFirst().value();
        double lastValue = currentWindow.getLast().value();
        long firstTs = currentWindow.getFirst().timestamp().toEpochMilli();
        long lastTs = currentWindow.getLast().timestamp().toEpochMilli();
        if (lastTs == firstTs) {
            return null;
        }

        double delta = lastValue - firstValue;
        double tsDurationSeconds = (lastTs - firstTs) / 1000.0;
        double windowSizeFactor = secondsInWindow / tsDurationSeconds;
        if (delta < 0) {
            return new double[] { delta * windowSizeFactor, delta };
        } else {
            return new double[] { delta, delta * windowSizeFactor };
        }
    }

    /**
     * Computes a reference idelta matching {@code IdeltaAggregator#evaluateFinal}.
     * Uses only the last two samples; no adjacent-window data, no temporality handling.
     */
    static Double computeReferenceIdelta(List<TimestampedValue> currentWindow) {
        if (currentWindow == null || currentWindow.size() < 2) {
            return null;
        }
        return currentWindow.getLast().value() - currentWindow.get(currentWindow.size() - 2).value();
    }

    /**
     * Calculates a delta-based aggregation (rate, irate, increase, delta, idelta) for a single time window.
     */
    static RateStats calculateDeltaAggregation(
        List<Map<String, List<TimestampedValue>>> allWindows,
        int offset,
        Integer secondsInWindow,
        DeltaAgg deltaAgg
    ) {
        List<RateRange> allRates = allWindows.get(offset).entrySet().stream().map(entry -> {
            String timeseriesId = entry.getKey();
            List<TimestampedValue> points = new ArrayList<>(entry.getValue());
            points.sort(Comparator.comparing(TimestampedValue::timestamp));
            Temporality temporality = getCounterTemporality(timeseriesId);

            long millisInWindow = secondsInWindow * 1000L;
            long currentBucketStartMs = points.getFirst().timestamp().toEpochMilli() / millisInWindow * millisInWindow;

            TimestampedValue lastInPrev = findLastPoint(allWindows, offset - 1, timeseriesId);
            TimestampedValue firstInNext = findFirstPoint(allWindows, offset + 1, timeseriesId);

            // The allWindows list only contains entries for non-empty buckets, so offset-1 / offset+1
            // may point to a non-adjacent time bucket when there are gaps in the data. ES only
            // interpolates with the immediately adjacent bucket, so discard points from distant buckets.
            if (lastInPrev != null) {
                long prevBucket = lastInPrev.timestamp().toEpochMilli() / millisInWindow * millisInWindow;
                if (prevBucket != currentBucketStartMs - millisInWindow) {
                    lastInPrev = null;
                }
            }
            if (firstInNext != null) {
                long nextBucket = firstInNext.timestamp().toEpochMilli() / millisInWindow * millisInWindow;
                if (nextBucket != currentBucketStartMs + millisInWindow) {
                    firstInNext = null;
                }
            }

            if (deltaAgg == DeltaAgg.DELTA) {
                double[] deltaRange = computeReferenceDelta(points, secondsInWindow);
                if (deltaRange == null) {
                    return null;
                }
                double tol = 0.001;
                double lo = deltaRange[0] < 0 ? deltaRange[0] * (1 + tol) : deltaRange[0] * (1 - tol);
                double hi = deltaRange[1] < 0 ? deltaRange[1] * (1 - tol) : deltaRange[1] * (1 + tol);
                return new RateRange(lo, hi);
            }

            Double value = switch (deltaAgg) {
                case RATE -> computeReferenceRateOrIncrease(lastInPrev, points, firstInNext, temporality, secondsInWindow, true);
                case INCREASE -> computeReferenceRateOrIncrease(lastInPrev, points, firstInNext, temporality, secondsInWindow, false);
                case IRATE -> computeReferenceIrate(points, temporality);
                case IDELTA -> computeReferenceIdelta(points);
                default -> throw new IllegalStateException("Unexpected delta agg: " + deltaAgg);
            };
            if (value == null || value.isNaN()) {
                return null;
            }
            double tol = 0.001;
            if (value == 0.0) {
                return new RateRange(0.0, 0.0);
            }
            double lo = value * (1 - tol);
            double hi = value * (1 + tol);
            return value < 0 ? new RateRange(hi, lo) : new RateRange(lo, hi);
        }).filter(Objects::nonNull).toList();
        if (allRates.isEmpty()) {
            return new RateStats(0L, null, null, null, null);
        }
        return new RateStats(
            (long) allRates.size(),
            allRates.stream().max(RateRange::compareToFindingMax).orElseThrow(),
            new RateRange(
                allRates.stream().mapToDouble(r -> r.lower).average().orElseThrow(),
                allRates.stream().mapToDouble(r -> r.upper).average().orElseThrow()
            ),
            allRates.stream().min(RateRange::compareTo).orElseThrow(),
            new RateRange(allRates.stream().mapToDouble(r -> r.lower).sum(), allRates.stream().mapToDouble(r -> r.upper).sum())
        );
    }

    void putTSDBIndexTemplate(List<String> patterns, @Nullable String mappingString) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        settingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, ESTestCase.randomIntBetween(1, 5));
        settingsBuilder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2025-07-31T00:00:00Z");
        settingsBuilder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-07-31T12:00:00Z");
        settingsBuilder.put(
            IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.getKey(),
            "attributes." + TSDataGenerationHelper.TEMPORALITY_ATTRIBUTE_NAME
        );
        settingsBuilder.put(IndexSettings.SYNTHETIC_ID.getKey(), randomBoolean());
        CompressedXContent mappings = mappingString == null ? null : CompressedXContent.fromJSON(mappingString);
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            RandomizedTimeSeriesIT.DATASTREAM_NAME
        );
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(org.elasticsearch.cluster.metadata.Template.builder().settings(settingsBuilder).mappings(mappings))
                .metadata(null)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    @Before
    public void populateIndex() throws IOException {
        List<Temporality> allowedTemporalities = randomNonEmptySubsetOf(Arrays.asList(Temporality.DELTA, Temporality.CUMULATIVE, null));
        dataGenerationHelper = new TSDataGenerationHelper(NUM_DOCS, TIME_RANGE_SECONDS, allowedTemporalities);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(dataGenerationHelper.mapping.raw());
        final String jsonMappings = Strings.toString(builder);

        putTSDBIndexTemplate(List.of(DATASTREAM_NAME + "*"), jsonMappings);
        // Now we can push data into the data stream.
        for (int i = 0; i < NUM_DOCS; i++) {
            var document = dataGenerationHelper.generateDocument(Map.of());
            if (documents == null) {
                documents = new ArrayList<>();
            }
            var indexRequest = client().prepareIndex(DATASTREAM_NAME).setOpType(DocWriteRequest.OpType.CREATE).setSource(document);
            indexRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.get();
            documents.add(document);
        }
    }

    void checkWithin(Double actual, RateRange expected) {
        if (expected == null) {
            assertThat(actual, equalTo(null));
            return;
        }
        assertThat(actual, allOf(lessThanOrEqualTo(expected.upper), not(lessThan(expected.lower))));
    }

    void assertNoFailedWindows(List<String> failedWindows, List<List<Object>> rows, String agg) {
        if (failedWindows.isEmpty() == false) {
            var pctFailures = (double) failedWindows.size() / rows.size() * 100;
            var failureDetails = String.join("\n", failedWindows);
            if (failureDetails.length() > 2000) {
                failureDetails = failureDetails.substring(0, 2000) + "\n... (truncated)";
            }
            throw new AssertionError(
                "Failed. Agg: " + agg + " | Failed windows: " + failedWindows.size() + " windows(" + pctFailures + "%):\n" + failureDetails
            );
        }
    }

    /**
     * This test validates Rate metrics aggregation with grouping by time bucket and a subset of dimensions.
     * The subset of dimensions is a random subset of the dimensions present in the data.
     * The test checks that the count, max, min, and avg values of the rate metric - and calculates
     * the same values from the documents in the group.
     */
    public void testRateGroupBySubset() {
        var deltaAgg = ESTestCase.randomFrom(DeltaAgg.values());
        var metricName = DELTA_AGG_METRIC_MAP.get(deltaAgg);
        var window = ESTestCase.randomFrom(WINDOW_OPTIONS);
        var dimensions = ESTestCase.randomSubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.isEmpty()
            ? ""
            : ", " + dimensions.stream().map(d -> "attributes.`" + d + "`").collect(Collectors.joining(", "));
        var query = String.format(Locale.ROOT, """
            TS %s
            | STATS count(<DELTAGG>(metrics.<METRIC>)),
                    max(<DELTAGG>(metrics.<METRIC>)),
                    avg(<DELTAGG>(metrics.<METRIC>)),
                    min(<DELTAGG>(metrics.<METRIC>)),
                    sum(<DELTAGG>(metrics.<METRIC>)),
                    values(<DELTAGG>(metrics.<METRIC>))
                BY tbucket=bucket(@timestamp, %s) %s
            | SORT tbucket
            """, DATASTREAM_NAME, window.label(), dimensionsStr)
            .replaceAll("<DELTAGG>", deltaAgg.functionName())
            .replaceAll("<METRIC>", metricName);
        try (var resp = run(query)) {
            List<List<Object>> rows = consumeRows(resp);
            List<String> failedWindows = new ArrayList<>();
            var groups = groupedRows(documents, dimensions, window.seconds());
            Map<String, List<Map<String, List<TimestampedValue>>>> windowsPerTimeseries = new HashMap<>();
            Map<String, List<List<Object>>> rowsPerTimeseries = new HashMap<>();
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, getTimestampIndex(query));
                String timeseriesId = getTimeseriesId(rowKey);
                if (timeseriesId.isEmpty()) {
                    continue;
                }
                var windowDataPoints = groups.get(rowKey);
                var docsPerTimeseries = groupByTimeseries(windowDataPoints, metricName);
                windowsPerTimeseries.computeIfAbsent(timeseriesId, k -> new ArrayList<>()).add(docsPerTimeseries);
                rowsPerTimeseries.computeIfAbsent(timeseriesId, k -> new ArrayList<>()).add(row);
            }
            for (var key : windowsPerTimeseries.keySet()) {
                var rowList = rowsPerTimeseries.get(key);
                var docsPerTimeseries = windowsPerTimeseries.get(key);
                for (int i = 0; i < rowList.size(); i++) {
                    var row = rowList.get(i);
                    var rateAgg = calculateDeltaAggregation(docsPerTimeseries, i, window.seconds(), deltaAgg);
                    try {
                        assertThat(row.getFirst(), equalTo(rateAgg.count));
                        checkWithin((Double) row.get(1), rateAgg.max);
                        checkWithin((Double) row.get(2), rateAgg.avg);
                        checkWithin((Double) row.get(3), rateAgg.min);
                        checkWithin((Double) row.get(4), rateAgg.sum);
                    } catch (AssertionError e) {
                        failedWindows.add(
                            "ROWS: "
                                + rows.size()
                                + "|Failed for row:\n"
                                + row
                                + "\nWanted: "
                                + rateAgg
                                + "\nException: "
                                + e.getMessage()
                                + "\nRow times and values:\n\tTS:"
                                + docsPerTimeseries.get(i)
                                    .values()
                                    .stream()
                                    .map(ts -> ts.stream().map(p -> p.timestamp() + "=" + p.value()).collect(Collectors.joining(", ")))
                                    .collect(Collectors.joining("\n\tTS:"))
                        );
                    }
                }
                assertNoFailedWindows(failedWindows, rows, deltaAgg.name());
            }
        }
    }

    /**
     * This test validates Rate metrics aggregation with grouping by time bucket only.
     * The test checks that the count, max, min, and avg values of the rate metric - and calculates
     * the same values from the documents in the group. Because there is no grouping by dimensions,
     * there is only one metric group per time bucket.
     */
    public void testRateGroupByNothing() {
        var groups = groupedRows(documents, List.of(), 60);
        List<Map<String, List<TimestampedValue>>> docsPerWindowPerTimeseries = new ArrayList<>();
        try (var resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS count(rate(metrics.counterl_hdd.bytes.read)),
                    max(rate(metrics.counterl_hdd.bytes.read)),
                    avg(rate(metrics.counterl_hdd.bytes.read)),
                    min(rate(metrics.counterl_hdd.bytes.read))
                BY tbucket=bucket(@timestamp, 1 minute)
            | SORT tbucket
            """, DATASTREAM_NAME))) {
            List<List<Object>> rows = consumeRows(resp);
            List<String> failedWindows = new ArrayList<>();
            for (List<Object> row : rows) {
                var windowStart = windowStart(row.get(4), SECONDS_IN_WINDOW);
                var windowDataPoints = groups.get(List.of(Long.toString(windowStart)));
                var docsPerTimeseries = groupByTimeseries(windowDataPoints, "counterl_hdd.bytes.read");
                docsPerWindowPerTimeseries.add(docsPerTimeseries);
            }
            for (int i = 0; i < rows.size(); i++) {
                var row = rows.get(i);
                var rateAgg = calculateDeltaAggregation(docsPerWindowPerTimeseries, i, SECONDS_IN_WINDOW, DeltaAgg.RATE);
                try {
                    assertThat(row.getFirst(), equalTo(rateAgg.count));
                    checkWithin((Double) row.get(1), rateAgg.max);
                    checkWithin((Double) row.get(2), rateAgg.avg);
                    checkWithin((Double) row.get(3), rateAgg.min);
                } catch (AssertionError e) {
                    failedWindows.add("Failed for row:\n" + row + "\nWanted: " + rateAgg + "\nException: " + e.getMessage());
                }
            }
            assertNoFailedWindows(failedWindows, rows, "RATE");
        }
    }

    public void testGaugeGroupByRandomAndRandomAgg() {
        var randomWindow = ESTestCase.randomFrom(WINDOW_OPTIONS);
        var dimensions = ESTestCase.randomSubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.isEmpty()
            ? ""
            : ", " + dimensions.stream().map(d -> "attributes.`" + d + "`").collect(Collectors.joining(", "));
        var metricName = ESTestCase.randomFrom(List.of("gaugel_hdd.bytes.used", "gauged_cpu.percent"));
        var selectedAggs = ESTestCase.randomSubsetOf(2, Agg.values());
        var aggExpression = String.format(
            Locale.ROOT,
            "%s(%s_over_time(metrics.%s))",
            selectedAggs.get(0),
            selectedAggs.get(1),
            metricName
        );
        // TODO: Remove WHERE clause after fixing https://github.com/elastic/elasticsearch/issues/129524
        var query = String.format(Locale.ROOT, """
            TS %s
            | WHERE %s IS NOT NULL
            | STATS
                %s
                BY tbucket=bucket(@timestamp, %s) %s
            | SORT tbucket
            """, DATASTREAM_NAME, metricName, aggExpression, randomWindow.label(), dimensionsStr);
        try (EsqlQueryResponse resp = run(query)) {
            var groups = groupedRows(documents, dimensions, randomWindow.seconds());
            List<List<Object>> rows = consumeRows(resp);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, getTimestampIndex(query));
                var tsGroups = groupByTimeseries(groups.get(rowKey), metricName);
                Object expectedVal = aggregatePerTimeseries(tsGroups, selectedAggs.get(0), selectedAggs.get(1));
                Double actualVal = switch (row.get(0)) {
                    case Long l -> l.doubleValue();
                    case Double d -> d;
                    case null -> null;
                    default -> throw new IllegalStateException(
                        "Unexpected value type: " + row.get(0) + " of class " + row.get(0).getClass()
                    );
                };
                try {
                    switch (expectedVal) {
                        case Double dVal -> assertThat(actualVal, closeTo(dVal, dVal * 0.01));
                        case Long lVal -> assertThat(actualVal, closeTo(lVal.doubleValue(), lVal * 0.01));
                        case null -> assertThat(actualVal, equalTo(null));
                        default -> throw new IllegalStateException(
                            "Unexpected value type: " + expectedVal + " of class " + expectedVal.getClass()
                        );
                    }
                } catch (AssertionError e) {
                    throw new AssertionError(
                        "Failed for aggregations:\n"
                            + selectedAggs
                            + " with total dimensions for grouping: "
                            + dimensions.size()
                            + " on metric "
                            + metricName
                            + "\nWanted val: "
                            + expectedVal
                            + "\nGot val: "
                            + actualVal
                            + "\nException: "
                            + e.getMessage(),
                        e
                    );
                }
            }
        }
    }

    /**
     * This test validates Gauge metrics aggregation with grouping by time bucket and a subset of dimensions.
     * The subset of dimensions is a random subset of the dimensions present in the data.
     * The test checks that the max, min, and avg values of the gauge metric - and calculates
     * the same values from the documents in the group.
     */
    public void testGroupBySubset() {
        var dimensions = ESTestCase.randomNonEmptySubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.stream().map(d -> "attributes.`" + d + "`").collect(Collectors.joining(", "));
        var query = String.format(Locale.ROOT, """
            TS %s
            | STATS
                max(max_over_time(metrics.gaugel_hdd.bytes.used)),
                min(min_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(count_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(sum_over_time(metrics.gaugel_hdd.bytes.used)),
                avg(avg_over_time(metrics.gaugel_hdd.bytes.used)),
                count(count_over_time(metrics.gaugel_hdd.bytes.used))
                BY tbucket=bucket(@timestamp, 1 minute), %s
            | SORT tbucket
            """, DATASTREAM_NAME, dimensionsStr);
        try (EsqlQueryResponse resp = run(query)) {
            var groups = groupedRows(documents, dimensions, 60);
            List<List<Object>> rows = consumeRows(resp);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, getTimestampIndex(query));
                var tsGroups = groupByTimeseries(groups.get(rowKey), "gaugel_hdd.bytes.used");
                Function<Object, Double> toDouble = cell -> switch (cell) {
                    case Long l -> l.doubleValue();
                    case Double d -> d;
                    case null -> null;
                    default -> throw new IllegalStateException("Unexpected value type: " + cell + " of class " + cell.getClass());
                };
                assertThat(toDouble.apply(row.get(0)), equalTo(aggregatePerTimeseries(tsGroups, Agg.MAX, Agg.MAX)));
                assertThat(toDouble.apply(row.get(1)), equalTo(aggregatePerTimeseries(tsGroups, Agg.MIN, Agg.MIN)));
                assertThat(toDouble.apply(row.get(2)), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.COUNT)));
                assertThat(toDouble.apply(row.get(3)), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.SUM)));
                var avg = (Double) aggregatePerTimeseries(tsGroups, Agg.AVG, Agg.AVG);
                assertThat((Double) row.get(4), row.get(4) == null ? equalTo(null) : closeTo(avg, avg * 0.01));
                // assertThat(row.get(6), equalTo(aggregatePerTimeseries(tsGroups, Agg.COUNT, Agg.COUNT).longValue()));
            }
        }
    }

    /**
     * This test validates Gauge metrics aggregation with grouping by time bucket only.
     * The test checks that the max, min, and avg values of the gauge metric - and calculates
     * the same values from the documents in the group. Because there is no grouping by dimensions,
     * there is only one metric group per time bucket.
     */
    public void testGroupByNothing() {
        try (EsqlQueryResponse resp = run(String.format(Locale.ROOT, """
            TS %s
            | STATS
                max(max_over_time(metrics.gaugel_hdd.bytes.used)),
                min(min_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(count_over_time(metrics.gaugel_hdd.bytes.used)),
                sum(sum_over_time(metrics.gaugel_hdd.bytes.used)),
                avg(avg_over_time(metrics.gaugel_hdd.bytes.used)),
                count(count_over_time(metrics.gaugel_hdd.bytes.used))
                BY tbucket=bucket(@timestamp, 1 minute)
            | SORT tbucket
            """, DATASTREAM_NAME))) {
            List<List<Object>> rows = consumeRows(resp);
            var groups = groupedRows(documents, List.of(), 60);
            for (List<Object> row : rows) {
                var windowStart = windowStart(row.get(6), 60);
                var tsGroups = groupByTimeseries(groups.get(List.of(Long.toString(windowStart))), "gaugel_hdd.bytes.used");
                Function<Object, Double> toDouble = cell -> switch (cell) {
                    case Long l -> l.doubleValue();
                    case Double d -> d;
                    case null -> null;
                    default -> throw new IllegalStateException("Unexpected value type: " + cell + " of class " + cell.getClass());
                };
                assertThat(toDouble.apply(row.get(0)), equalTo(aggregatePerTimeseries(tsGroups, Agg.MAX, Agg.MAX)));
                assertThat(toDouble.apply(row.get(1)), equalTo(aggregatePerTimeseries(tsGroups, Agg.MIN, Agg.MIN)));
                assertThat(toDouble.apply(row.get(2)), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.COUNT)));
                assertThat(toDouble.apply(row.get(3)), equalTo(aggregatePerTimeseries(tsGroups, Agg.SUM, Agg.SUM)));
                var avg = (Double) aggregatePerTimeseries(tsGroups, Agg.AVG, Agg.AVG);
                assertThat((Double) row.get(4), row.get(4) == null ? equalTo(null) : closeTo(avg, avg * 0.01));
                // assertThat(row.get(6), equalTo(aggregatePerTimeseries(tsGroups, Agg.COUNT, Agg.COUNT).longValue()));
            }
        }
    }
}
