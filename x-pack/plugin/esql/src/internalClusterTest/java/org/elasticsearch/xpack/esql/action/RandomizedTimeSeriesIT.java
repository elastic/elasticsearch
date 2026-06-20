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
            EsqlPluginWithEnterpriseOrTrialLicense.class,
            TestEncryptionServicePlugin.class
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

    /**
     * Calculates a delta-based aggregation (rate, irate, increase, delta, idelta) for a single time window,
     * using adjacent windows for boundary interpolation.
     *
     * @param allWindows ordered list of time windows; each window is a map keyed by timeseries identifier
     *                   to the data points in that window
     * @param offset index into {@code allWindows} for the window to compute
     * @param secondsInWindow time bucket width in seconds
     * @param deltaAgg the delta aggregation type to compute
     */
    static RateStats calculateDeltaAggregation(
        List<Map<String, List<TimestampedValue>>> allWindows,
        int offset,
        Integer secondsInWindow,
        DeltaAgg deltaAgg
    ) {
        List<RateRange> allRates = allWindows.get(offset).entrySet().stream().map(entry -> {
            String timeseriesId = entry.getKey();
            var timeseriesPointsInWindow = new ArrayList<>(entry.getValue());
            timeseriesPointsInWindow.sort(Comparator.comparing(TimestampedValue::timestamp));

            boolean addedLowerBoundary = false;
            boolean addedUpperBoundary = false;
            if (deltaAgg.equals(DeltaAgg.RATE) || deltaAgg.equals(DeltaAgg.INCREASE)) {
                if (offset > 0) {
                    var previousWindow = allWindows.get(offset - 1).get(timeseriesId);
                    if (previousWindow != null && previousWindow.isEmpty() == false) {
                        addedLowerBoundary = addBoundaryPoint(timeseriesPointsInWindow, previousWindow, secondsInWindow, true);
                    }
                }
                if (offset < allWindows.size() - 1) {
                    var nextWindow = allWindows.get(offset + 1).get(timeseriesId);
                    if (nextWindow != null && nextWindow.isEmpty() == false) {
                        addedUpperBoundary = addBoundaryPoint(timeseriesPointsInWindow, nextWindow, secondsInWindow, false);
                    }
                }
            }
            if (timeseriesPointsInWindow.size() < 2) {
                if ((deltaAgg.equals(DeltaAgg.RATE) || deltaAgg.equals(DeltaAgg.INCREASE))
                    && timeseriesPointsInWindow.size() == 1
                    && timeseriesPointsInWindow.getFirst().timestamp().toEpochMilli() % (secondsInWindow * 1000L) == 0
                    && offset > 0) {
                    // Value at lower boundary is present, check if there's one in the previous window to use.
                    addLastPointFromLowerWindow(timeseriesPointsInWindow, allWindows.get(offset - 1).get(timeseriesId), secondsInWindow);
                    // For INCREASE, return 0 if there is a previous bucket because
                    // the increase was already accounted for in the previous bucket.
                    // For RATE, we still need to calculate the rate using interpolation from the previous bucket.
                    if (timeseriesPointsInWindow.size() == 2 && deltaAgg.equals(DeltaAgg.INCREASE)) {
                        return new RateRange(0.0, 0.0);
                    }
                }
                if (timeseriesPointsInWindow.size() < 2) {
                    return null;
                }
            }
            var firstTs = timeseriesPointsInWindow.getFirst().timestamp();
            var lastTs = timeseriesPointsInWindow.getLast().timestamp();
            var tsDurationSeconds = (lastTs.toEpochMilli() - firstTs.toEpochMilli()) / 1000.0;
            if (deltaAgg.equals(DeltaAgg.IRATE)) {
                var lastVal = timeseriesPointsInWindow.getLast().value();
                var secondLast = timeseriesPointsInWindow.get(timeseriesPointsInWindow.size() - 2);
                var secondLastVal = secondLast.value();
                var irate = (lastVal >= secondLastVal ? lastVal - secondLastVal : lastVal) / (lastTs.toEpochMilli() - secondLast.timestamp()
                    .toEpochMilli()) * 1000;
                return new RateRange(irate * 0.999, irate * 1.001); // Add 0.1% tolerance
            } else if (deltaAgg.equals(DeltaAgg.DELTA)) {
                var firstVal = timeseriesPointsInWindow.getFirst().value();
                var lastVal = timeseriesPointsInWindow.getLast().value();
                var delta = lastVal - firstVal;
                var windowSizeFactor = secondsInWindow / tsDurationSeconds;
                if (delta < 0) {
                    return new RateRange(delta * windowSizeFactor * 1.001, delta * 0.999); // Add 0.1% tolerance
                } else {
                    return new RateRange(delta * 0.999, delta * windowSizeFactor * 1.001); // Add 0.1% tolerance
                }
            } else if (deltaAgg.equals(DeltaAgg.IDELTA)) {
                var lastVal = timeseriesPointsInWindow.getLast().value();
                var secondLastVal = timeseriesPointsInWindow.get(timeseriesPointsInWindow.size() - 2).value();
                var idelta = lastVal - secondLastVal;
                if (idelta < 0) {
                    return new RateRange(idelta * 1.001, idelta * 0.999); // Add 0.1% tolerance
                } else {
                    return new RateRange(idelta * 0.999, idelta * 1.001); // Add 0.1% tolerance
                }
            }
            assert deltaAgg == DeltaAgg.RATE || deltaAgg == DeltaAgg.INCREASE;
            double lastValue = 0.0;
            boolean first = true;
            double counterGrowth = 0.0;
            for (TimestampedValue point : timeseriesPointsInWindow) {
                double currentValue = point.value();
                if (first) {
                    lastValue = currentValue;
                    first = false;
                    continue;
                }
                if (currentValue > lastValue) {
                    counterGrowth += currentValue - lastValue;
                } else if (currentValue < lastValue) {
                    counterGrowth += currentValue;
                }
                lastValue = currentValue;
            }

            // Account for extrapolation in case there are no adjacent buckets.
            if (timeseriesPointsInWindow.size() > 2) {
                if (addedLowerBoundary && addedUpperBoundary == false) {
                    firstTs = timeseriesPointsInWindow.get(1).timestamp();
                } else if (addedLowerBoundary == false && addedUpperBoundary) {
                    lastTs = timeseriesPointsInWindow.get(timeseriesPointsInWindow.size() - 2).timestamp();
                }
                tsDurationSeconds = (lastTs.toEpochMilli() - firstTs.toEpochMilli()) / 1000.0;
            }

            if (deltaAgg.equals(DeltaAgg.INCREASE)) {
                // TODO: get tighter bounds by applying interpolation instead of median between adjacent buckets
                return new RateRange(counterGrowth * 0.9, counterGrowth * secondsInWindow / tsDurationSeconds * 1.1);
            } else {
                double lowBound = counterGrowth / secondsInWindow * 0.9;
                double highBound = counterGrowth / tsDurationSeconds * 1.1;
                return new RateRange(lowBound, highBound);
            }
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

    /**
     * Adds an interpolated boundary point to a timeseries using data from the same timeseries in an
     * adjacent window.
     *
     * @param currentWindow mutable list of data points for one timeseries in the current window (modified in place)
     * @param adjacentWindow data points for the same timeseries in the adjacent window
     * @return {@code true} if a boundary point was added or the reference point is already on the boundary
     */
    private static boolean addBoundaryPoint(
        List<TimestampedValue> currentWindow,
        List<TimestampedValue> adjacentWindow,
        int secondsInWindow,
        boolean isLowerBoundary
    ) {
        var referencePoint = isLowerBoundary ? currentWindow.getFirst() : currentWindow.getLast();
        if (isLowerBoundary && referencePoint.timestamp().toEpochMilli() % (secondsInWindow * 1000L) == 0) {
            return true;
        }
        if (instantsInAdjacentWindows(adjacentWindow.getFirst().timestamp(), referencePoint.timestamp(), secondsInWindow) == false) {
            return false;
        }
        TimestampedValue otherValue = null;
        long otherTimestamp = 0;
        for (var point : adjacentWindow) {
            long timestamp = point.timestamp().toEpochMilli();
            if (otherValue == null
                || (timestamp > otherTimestamp && isLowerBoundary)
                || (timestamp < otherTimestamp && isLowerBoundary == false)) {
                otherTimestamp = timestamp;
                otherValue = point;
            }
        }
        if (isLowerBoundary) {
            currentWindow.addFirst(interpolateAtLowerBoundary(otherValue, referencePoint, secondsInWindow));
        } else {
            currentWindow.addLast(interpolateAtUpperBoundary(referencePoint, otherValue, secondsInWindow));
        }
        return true;
    }

    /**
     * Prepends the latest data point from the same currentWindow in the lower (previous) window,
     * used when only a single boundary-aligned point exists in the current window.
     *
     * @param currentWindow mutable list of data points for one currentWindow in the current window (modified in place)
     * @param previousWindow data points for the same currentWindow in the previous window, or {@code null} if absent
     */
    private static void addLastPointFromLowerWindow(
        List<TimestampedValue> currentWindow,
        @Nullable List<TimestampedValue> previousWindow,
        int secondsInWindow
    ) {
        if (previousWindow == null || previousWindow.isEmpty()) {
            return;
        }
        Instant referenceTimestamp = currentWindow.getFirst().timestamp();
        if (instantsInAdjacentWindows(previousWindow.getFirst().timestamp(), referenceTimestamp, secondsInWindow) == false) {
            return;
        }
        TimestampedValue lowerPoint = null;
        long lowerTimestampMs = 0;
        for (var point : previousWindow) {
            long timestamp = point.timestamp().toEpochMilli();
            if (lowerPoint == null || timestamp > lowerTimestampMs) {
                lowerTimestampMs = timestamp;
                lowerPoint = point;
            }
        }
        if (lowerPoint != null) {
            currentWindow.addFirst(lowerPoint);
        }
    }

    private static boolean instantsInAdjacentWindows(Instant first, Instant second, int secondsInWindow) {
        long firstRounded = first.getEpochSecond() / secondsInWindow * secondsInWindow;
        long secondRounded = second.getEpochSecond() / secondsInWindow * secondsInWindow;
        long delta = Math.abs(firstRounded - secondRounded);
        return delta == secondsInWindow;
    }

    private static TimestampedValue interpolateAtLowerBoundary(
        TimestampedValue lowerPoint,
        TimestampedValue upperPoint,
        int secondsInWindow
    ) {
        final double valueDelta;
        final double baseValue;
        if (upperPoint.value() >= lowerPoint.value()) {
            valueDelta = upperPoint.value() - lowerPoint.value();
            baseValue = lowerPoint.value();
        } else {
            // Counter reset.
            valueDelta = upperPoint.value();
            baseValue = 0;
        }
        final double timeDelta = (upperPoint.timestamp().toEpochMilli() - lowerPoint.timestamp().toEpochMilli()) / 1000.0;
        final double slope = valueDelta / timeDelta;
        final long lowerBoundaryTimeSeconds = upperPoint.timestamp().getEpochSecond() / secondsInWindow * secondsInWindow;
        final double lowerBoundaryValue = baseValue + slope * (lowerBoundaryTimeSeconds - lowerPoint.timestamp().toEpochMilli() / 1000.0);
        return new TimestampedValue(Instant.ofEpochSecond(lowerBoundaryTimeSeconds), lowerBoundaryValue);
    }

    private static TimestampedValue interpolateAtUpperBoundary(
        TimestampedValue lowerPoint,
        TimestampedValue upperPoint,
        int secondsInWindow
    ) {
        final double valueDelta;
        if (upperPoint.value() >= lowerPoint.value()) {
            valueDelta = upperPoint.value() - lowerPoint.value();
        } else {
            // Counter reset.
            valueDelta = upperPoint.value();
        }
        final double timeDelta = (upperPoint.timestamp().toEpochMilli() - lowerPoint.timestamp().toEpochMilli()) / 1000.0;
        final double slope = valueDelta / timeDelta;
        final long upperBoundaryTimeSeconds = upperPoint.timestamp().getEpochSecond() / secondsInWindow * secondsInWindow;
        final double upperBoundaryValue = lowerPoint.value() + slope * (upperBoundaryTimeSeconds - lowerPoint.timestamp().toEpochMilli()
            / 1000.0);
        return new TimestampedValue(Instant.ofEpochSecond(upperBoundaryTimeSeconds), upperBoundaryValue);
    }

    void putTSDBIndexTemplate(List<String> patterns, @Nullable String mappingString) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        settingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, ESTestCase.randomIntBetween(1, 5));
        settingsBuilder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2025-07-31T00:00:00Z");
        settingsBuilder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-07-31T12:00:00Z");
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
        dataGenerationHelper = new TSDataGenerationHelper(NUM_DOCS, TIME_RANGE_SECONDS);
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
