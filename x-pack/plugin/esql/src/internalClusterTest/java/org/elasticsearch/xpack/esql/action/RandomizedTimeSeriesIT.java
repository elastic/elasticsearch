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
import org.elasticsearch.core.Tuple;
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
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
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
    private static final Long NUM_DOCS = 20L;
    private static final Long TIME_RANGE_SECONDS = 60L;
    private static final String DATASTREAM_NAME = "tsit_ds";
    private static final Integer SECONDS_IN_WINDOW = 60;
    private static final List<Tuple<String, Integer>> WINDOW_OPTIONS = List.of(
        Tuple.tuple("10 seconds", 10),
        Tuple.tuple("30 seconds", 30),
        Tuple.tuple("1 minute", 60),
        Tuple.tuple("2 minutes", 120),
        Tuple.tuple("3 minutes", 180),
        Tuple.tuple("5 minutes", 300),
        Tuple.tuple("10 minutes", 600),
        Tuple.tuple("30 minutes", 1800),
        Tuple.tuple("1 hour", 3600)
    );
    private static final List<Tuple<String, DeltaAgg>> DELTA_AGG_OPTIONS = List.of(
        Tuple.tuple("rate", DeltaAgg.RATE),
        Tuple.tuple("irate", DeltaAgg.IRATE),
        Tuple.tuple("increase", DeltaAgg.INCREASE),
        Tuple.tuple("idelta", DeltaAgg.IDELTA),
        Tuple.tuple("delta", DeltaAgg.DELTA),
        Tuple.tuple("rate", DeltaAgg.NEW_RATE)
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
        "gaugel_hdd.bytes.used",
        DeltaAgg.NEW_RATE,
        "counterl_hdd.bytes.read"
    );

    private List<XContentBuilder> documents;
    private TSDataGenerationHelper dataGenerationHelper;

    List<List<Object>> consumeRows(EsqlQueryResponse resp) {
        List<List<Object>> rows = new ArrayList<>();
        resp.rows().forEach(rowIter -> {
            List<Object> row = new ArrayList<>();
            rowIter.forEach(row::add);
            rows.add(row);
        });
        return rows;
    }

    Map<List<String>, List<Map<String, Object>>> groupedRows(
        List<XContentBuilder> docs,
        List<String> groupingAttributes,
        int secondsInWindow
    ) {
        Map<List<String>, List<Map<String, Object>>> groupedMap = new HashMap<>();
        for (XContentBuilder doc : docs) {
            Map<String, Object> docMap = XContentHelper.convertToMap(BytesReference.bytes(doc), false, XContentType.JSON).v2();
            @SuppressWarnings("unchecked")
            List<String> groupingPairs = groupingAttributes.stream()
                .map(
                    attr -> Tuple.tuple(
                        attr,
                        ((Map<String, Object>) docMap.getOrDefault("attributes", Map.of())).getOrDefault(attr, "").toString()
                    )
                )
                .filter(val -> val.v2().isEmpty() == false) // Filter out empty values
                .map(tup -> tup.v1() + ":" + tup.v2())
                .toList();
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

    static List<Integer> valuesInWindow(List<Map<String, Object>> pointsInGroup, String metricName) {
        @SuppressWarnings("unchecked")
        var values = pointsInGroup.stream()
            .map(doc -> ((Map<String, Integer>) doc.get("metrics")).get(metricName))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return values;
    }

    static Map<String, List<Tuple<String, Tuple<Instant, Double>>>> groupByTimeseries(
        List<Map<String, Object>> pointsInGroup,
        String metricName
    ) {
        return pointsInGroup.stream()
            .filter(doc -> doc.containsKey("metrics") && ((Map<String, Object>) doc.get("metrics")).containsKey(metricName))
            .map(doc -> {
                String docKey = ((Map<String, Object>) doc.get("attributes")).entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(","));
                var docTs = Instant.parse((String) doc.get("@timestamp"));
                var docValue = switch (((Map<String, Object>) doc.get("metrics")).get(metricName)) {
                    case Integer i -> i.doubleValue();
                    case Long l -> l.doubleValue();
                    case Float f -> f.doubleValue();
                    case Double d -> d;
                    default -> throw new IllegalStateException(
                        "Unexpected value type: "
                            + ((Map<String, Object>) doc.get("metrics")).get(metricName)
                            + " of class "
                            + ((Map<String, Object>) doc.get("metrics")).get(metricName).getClass()
                    );
                };
                return new Tuple<>(docKey, new Tuple<>(docTs, docValue));
            })
            .collect(Collectors.groupingBy(Tuple::v1));
    }

    static Object aggregatePerTimeseries(
        Map<String, List<Tuple<String, Tuple<Instant, Double>>>> timeseries,
        Agg crossAgg,
        Agg timeseriesAgg
    ) {
        var res = timeseries.values().stream().map(timeseriesList -> {
            List<Double> values = timeseriesList.stream().map(t -> t.v2().v2()).collect(Collectors.toList());
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
        return List.of(DataStreamsPlugin.class, LocalStateCompositeXPackPlugin.class, AggregateMetricMapperPlugin.class, EsqlPlugin.class);
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
        RATE,
        IRATE,
        IDELTA,
        INCREASE,
        DELTA,
        NEW_RATE
    }

    // A record that holds min, max, avg, count and sum of rates calculated from a timeseries.
    record RateStats(Long count, RateRange max, RateRange avg, RateRange min, RateRange sum) {}

    static RateStats calculateDeltaAggregation(
        Collection<List<Tuple<String, Tuple<Instant, Double>>>> allTimeseries,
        Integer secondsInWindow,
        DeltaAgg deltaAgg,
        Collection<List<Tuple<String, Tuple<Instant, Double>>>> previousWindowTimeseries
    ) {
        List<RateRange> allRates = allTimeseries.stream().map(timeseries -> {
            if (timeseries.size() < 2 && (deltaAgg == DeltaAgg.NEW_RATE && previousWindowTimeseries != null) == false) {
                // NEW_RATE is allowed to have a single data point if we have previous window data (that we can then use to extrapolate)
                // otherwise, we are unable to calculate a rate with less than 2 data points
                return null;
            }
            // Sort the timeseries by timestamp
            timeseries.sort(Comparator.comparing(t -> t.v2().v1()));
            var firstVal = timeseries.getFirst().v2().v2();
            var firstTs = timeseries.getFirst().v2().v1();
            var lastTs = timeseries.getLast().v2().v1();
            var lastVal = timeseries.getLast().v2().v2();
            var tsDurationSeconds = (lastTs.toEpochMilli() - firstTs.toEpochMilli()) / 1000.0;
            if (deltaAgg.equals(DeltaAgg.IRATE)) {
                var secondLastVal = timeseries.get(timeseries.size() - 2).v2().v2();
                var irate = (lastVal >= secondLastVal ? lastVal - secondLastVal : lastVal) / (lastTs.toEpochMilli() - timeseries.get(
                    timeseries.size() - 2
                ).v2().v1().toEpochMilli()) * 1000;
                return new RateRange(irate * 0.999, irate * 1.001); // Add 0.1% tolerance
            } else if (deltaAgg.equals(DeltaAgg.DELTA)) {
                var delta = lastVal - firstVal;
                // We must extrapolate the delta to the window size
                var windowSizeFactor = secondsInWindow / tsDurationSeconds;
                if (delta < 0) {
                    return new RateRange(delta * windowSizeFactor * 1.001, delta * 0.999); // Add 0.1% tolerance
                } else {
                    return new RateRange(delta * 0.999, delta * windowSizeFactor * 1.001); // Add 0.1% tolerance
                }
            } else if (deltaAgg.equals(DeltaAgg.IDELTA)) {
                var secondLastVal = timeseries.get(timeseries.size() - 2).v2().v2();
                var idelta = lastVal - secondLastVal;
                if (idelta < 0) {
                    return new RateRange(idelta * 1.001, idelta * 0.999); // Add 0.1% tolerance
                } else {
                    return new RateRange(idelta * 0.999, idelta * 1.001); // Add 0.1% tolerance
                }
            }
            double counterGrowth = getCounterGrowth(deltaAgg, timeseries);
            // We calculate the rate preemptively here, and then adjust based on the deltaAgg type below
            RateRange regularRate = new RateRange(
                counterGrowth / secondsInWindow * 0.99, // Add 1% tolerance to the lower bound
                counterGrowth == 0 ? 0 : counterGrowth / tsDurationSeconds * 1.01 // Add 1% tolerance to the upper bound
            );
            if (deltaAgg.equals(DeltaAgg.NEW_RATE)) {
                // We need to find the last value and timestamp from the previous window for this timeseries
                Double previousWindowLastValue = null;
                Instant previousWindowLastTimestamp = null;
                if (previousWindowTimeseries != null) {
                    for (List<Tuple<String, Tuple<Instant, Double>>> prevTs : previousWindowTimeseries) {
                        if (prevTs.getFirst().v1().equals(timeseries.getFirst().v1())) {
                            // This is the matching timeseries from the previous window
                            prevTs.sort(Comparator.comparing(t -> t.v2().v1()));
                            previousWindowLastValue = prevTs.getLast().v2().v2();
                            previousWindowLastTimestamp = prevTs.getLast().v2().v1();
                            break;
                        }
                    }
                }
                if (previousWindowLastValue != null && previousWindowLastTimestamp != null) {
                    // counterGrowth is LAST_VAL_CURRENT_WINDOW + RESETS - FIRST_VAL_CURRENT_WINDOW
                    // we want LAST_VAL_CURRENT_WINDOW + RESETS - LAST_VAL_PREVIOUS_WINDOW
                    // AND RESETS = FIRST_VAL_CURRENT_WINDOW > LAST_VAL_PREVIOUS_WINDOW ? RESETS : RESETS + FIRST_VAL_CURRENT_WINDOW
                    double timeDiffSeconds = (lastTs.toEpochMilli() - previousWindowLastTimestamp.toEpochMilli()) / 1000.0;
                    double valueDiff = firstVal >= previousWindowLastValue
                        ? counterGrowth + firstVal - previousWindowLastValue // no reset
                        : counterGrowth + firstVal + firstVal;
                    double newRate = valueDiff / timeDiffSeconds;
                    return new RateRange(newRate * 0.99, newRate * 1.01); // Add 1% tolerance
                } else if (regularRate.lower > 0 && regularRate.upper > 0) {
                    // Fallback to RATE calculation if no previous window data is available
                    // (this should be rare, only for the first window)
                    return regularRate;
                } else {
                    return null; // Cannot calculate NEW_RATE if we have no previous window data and the rate is non-positive
                }
            } else if (deltaAgg.equals(DeltaAgg.INCREASE)) {
                return new RateRange(
                    counterGrowth * 0.99, // INCREASE is RATE multiplied by the window size
                    // Upper bound is extrapolated to the window size
                    counterGrowth * secondsInWindow / tsDurationSeconds * 1.01
                );
            } else {
                return regularRate;
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

    private static double getCounterGrowth(DeltaAgg deltaAgg, List<Tuple<String, Tuple<Instant, Double>>> timeseries) {
        assert deltaAgg == DeltaAgg.RATE || deltaAgg == DeltaAgg.INCREASE || deltaAgg == DeltaAgg.NEW_RATE;
        Double lastValue = null;
        double counterGrowth = 0.0;
        for (Tuple<String, Tuple<Instant, Double>> point : timeseries) {
            var currentValue = point.v2().v2();
            if (currentValue == null) {
                throw new IllegalArgumentException("Null value in counter timeseries");
            }
            if (lastValue == null) {
                lastValue = point.v2().v2(); // Initialize with the first value
                continue;
            }
            if (currentValue > lastValue) {
                counterGrowth += currentValue - lastValue; // Incremental growth
            } else if (currentValue < lastValue) {
                // If the value decreased, we assume a reset and start counting from the current value
                counterGrowth += currentValue;
            }
            lastValue = currentValue; // Update last value for next iteration
        }
        return counterGrowth;
    }

    void putTSDBIndexTemplate(List<String> patterns, @Nullable String mappingString) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        settingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, ESTestCase.randomIntBetween(1, 5));
        settingsBuilder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2025-07-31T00:00:00Z");
        settingsBuilder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-07-31T12:00:00Z");
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

    void assertNoFailedWindows(List<String> failedWindows, List<List<Object>> rows, String agg, EsqlQueryResponse resp) {
        var pctFailures = (double) failedWindows.size() / rows.size() * 100;
        var failureDetails = String.join("\n", failedWindows);
        if (failureDetails.length() > 2000) {
            failureDetails = failureDetails.substring(0, 2000) + "\n... (truncated)";
        }
        StringBuilder queryResult = new StringBuilder();
        // Now we print the entire response for debugging
        resp.rows().forEach(row -> {
            List<String> rowList = new ArrayList<>();
            row.forEach(cell -> {
                queryResult.append(cell);
                queryResult.append(", ");
            });
            queryResult.append("\n");
        });
        if (failedWindows.isEmpty() == false) {
            throw new AssertionError(
                "Failed. Agg: "
                    + agg
                    + " | Failed windows: "
                    + failedWindows.size()
                    + " windows("
                    + pctFailures
                    + "%):\n"
                    + failureDetails
                    + "\nQuery result:\n"
                    + queryResult.toString()
            );
        }
        // System.out.println(
        // "Success! Agg: "
        // + agg
        // + " | Total windows: "
        // + rows.size()
        // + " | Failed windows: "
        // + failedWindows.size()
        // + " windows("
        // + pctFailures
        // + "%):\n"
        // + "\nQuery result:\n"
        // + queryResult.toString()
        // );
    }

    /**
     * This test validates Rate metrics aggregation with grouping by time bucket and a subset of dimensions.
     * The subset of dimensions is a random subset of the dimensions present in the data.
     * The test checks that the count, max, min, and avg values of the rate metric - and calculates
     * the same values from the documents in the group.
     */
    public void testRateGroupBySubset() {
        // var deltaAgg = ESTestCase.randomFrom(DELTA_AGG_OPTIONS);
        var deltaAgg = Tuple.tuple("rate", DeltaAgg.NEW_RATE);
        var metricName = DELTA_AGG_METRIC_MAP.get(deltaAgg.v2());
        var window = ESTestCase.randomFrom(WINDOW_OPTIONS);
        var windowSize = window.v2();
        var windowStr = window.v1();
        var dimensions = ESTestCase.randomSubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.isEmpty()
            ? ""
            : ", " + dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
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
            """, DATASTREAM_NAME, windowStr, dimensionsStr).replaceAll("<DELTAGG>", deltaAgg.v1()).replaceAll("<METRIC>", metricName);
        Map<List<String>, Map<String, List<Tuple<String, Tuple<Instant, Double>>>>> previousWindows = new HashMap<>();
        try (var resp = run(query)) {
            List<List<Object>> rows = consumeRows(resp);
            List<String> failedWindows = new ArrayList<>();
            var groups = groupedRows(documents, dimensions, windowSize);
            for (List<Object> row : rows) {
                var rowKey = getRowKey(row, dimensions, getTimestampIndex(query));
                var windowDataPoints = groups.get(rowKey);
                var docsPerTimeseries = groupByTimeseries(windowDataPoints, metricName);
                // The last element of the rowKey is the time bucket, we don't want to include it in the previousWindows key
                var previousWindowDocs = previousWindows.put(rowKey.subList(0, rowKey.size() - 1), docsPerTimeseries);
                var rateAgg = calculateDeltaAggregation(
                    docsPerTimeseries.values(),
                    windowSize,
                    deltaAgg.v2(),
                    previousWindowDocs == null ? null : previousWindowDocs.values()
                );
                try {
                    checkWithin((Double) row.get(1), rateAgg.max);
                    checkWithin((Double) row.get(2), rateAgg.avg);
                    checkWithin((Double) row.get(3), rateAgg.min);
                    checkWithin((Double) row.get(4), rateAgg.sum);
                    assertThat(row.getFirst(), equalTo(rateAgg.count));
                } catch (AssertionError e) {
                    failedWindows.add("ROWS: " + rows.size() + "|Failed for row:\n" + row + "\nWanted: " + rateAgg
                    // + "\nRow times and values:\n\tTS:"
                    // + docsPerTimeseries.values()
                    // .stream()
                    // .map(ts -> ts.stream().map(t -> t.v2().v1() + "=" + t.v2().v2()).collect(Collectors.joining(", ")))
                    // .collect(Collectors.joining("\n\tTS:"))
                        + "\nException: "
                        + e.getMessage());
                }
            }
            assertNoFailedWindows(failedWindows, rows, deltaAgg.v2().name(), resp);
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
                var rateAgg = calculateDeltaAggregation(docsPerTimeseries.values(), SECONDS_IN_WINDOW, DeltaAgg.RATE, null);
                try {
                    assertThat(row.getFirst(), equalTo(rateAgg.count));
                    checkWithin((Double) row.get(1), rateAgg.max);
                    checkWithin((Double) row.get(2), rateAgg.avg);
                    checkWithin((Double) row.get(3), rateAgg.min);
                } catch (AssertionError e) {
                    failedWindows.add("Failed for row:\n" + row + "\nWanted: " + rateAgg + "\nException: " + e.getMessage());
                }
            }
            assertNoFailedWindows(failedWindows, rows, "RATE", resp);
        }
    }

    public void testGaugeGroupByRandomAndRandomAgg() {
        var randomWindow = ESTestCase.randomFrom(WINDOW_OPTIONS);
        var windowSize = randomWindow.v2();
        var windowStr = randomWindow.v1();
        var dimensions = ESTestCase.randomSubsetOf(dataGenerationHelper.attributesForMetrics);
        var dimensionsStr = dimensions.isEmpty()
            ? ""
            : ", " + dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
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
            """, DATASTREAM_NAME, metricName, aggExpression, windowStr, dimensionsStr);
        try (EsqlQueryResponse resp = run(query)) {
            var groups = groupedRows(documents, dimensions, windowSize);
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
        var dimensionsStr = dimensions.stream().map(d -> "attributes." + d).collect(Collectors.joining(", "));
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
