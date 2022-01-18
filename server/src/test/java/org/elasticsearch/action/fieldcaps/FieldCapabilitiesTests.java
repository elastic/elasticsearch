/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesTests extends AbstractSerializingTestCase<FieldCapabilities> {
    private static final String FIELD_NAME = "field";

    @Override
    protected FieldCapabilities doParseInstance(XContentParser parser) throws IOException {
        return FieldCapabilities.fromXContent(FIELD_NAME, parser);
    }

    @Override
    protected FieldCapabilities createTestInstance() {
        return randomFieldCaps(FIELD_NAME);
    }

    @Override
    protected Writeable.Reader<FieldCapabilities> instanceReader() {
        return FieldCapabilities::new;
    }

    public void testBuilder() {
        FieldCapabilities.Builder builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true, false, false, null, Collections.emptyMap());
        builder.add("index2", false, true, false, false, null, Collections.emptyMap());
        builder.add("index3", false, true, false, false, null, Collections.emptyMap());

        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertThat(cap1.isDimension(), equalTo(false));
            assertNull(cap1.getMetricType());
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertNull(cap1.nonDimensionIndices());
            assertEquals(Collections.emptyMap(), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.isDimension(), equalTo(false));
            assertNull(cap2.getMetricType());
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertNull(cap2.nonDimensionIndices());
            assertEquals(Collections.emptyMap(), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, false, true, true, null, Collections.emptyMap());
        builder.add("index2", false, true, false, false, TimeSeriesParams.MetricType.counter, Collections.emptyMap());
        builder.add("index3", false, false, false, false, null, Collections.emptyMap());
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(false));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertThat(cap1.isDimension(), equalTo(false));
            assertNull(cap1.getMetricType());
            assertNull(cap1.indices());
            assertThat(cap1.nonSearchableIndices(), equalTo(new String[] { "index1", "index3" }));
            assertThat(cap1.nonAggregatableIndices(), equalTo(new String[] { "index2", "index3" }));
            assertThat(cap1.nonDimensionIndices(), equalTo(new String[] { "index2", "index3" }));
            assertEquals(Collections.emptyMap(), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(false));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.isDimension(), equalTo(false));
            assertNull(cap2.getMetricType());
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertThat(cap2.nonSearchableIndices(), equalTo(new String[] { "index1", "index3" }));
            assertThat(cap2.nonAggregatableIndices(), equalTo(new String[] { "index2", "index3" }));
            assertThat(cap2.nonDimensionIndices(), equalTo(new String[] { "index2", "index3" }));
            assertEquals(Collections.emptyMap(), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true, true, true, TimeSeriesParams.MetricType.counter, Collections.emptyMap());
        builder.add("index2", false, true, true, true, TimeSeriesParams.MetricType.counter, Map.of("foo", "bar"));
        builder.add("index3", false, true, true, true, TimeSeriesParams.MetricType.counter, Map.of("foo", "quux"));
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(true));
            assertThat(cap1.isDimension(), equalTo(true));
            assertThat(cap1.getMetricType(), equalTo(TimeSeriesParams.MetricType.counter));
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertNull(cap1.nonDimensionIndices());
            assertEquals(Map.of("foo", Set.of("bar", "quux")), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(true));
            assertThat(cap2.isDimension(), equalTo(true));
            assertThat(cap2.getMetricType(), equalTo(TimeSeriesParams.MetricType.counter));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertNull(cap2.nonDimensionIndices());
            assertEquals(Map.of("foo", Set.of("bar", "quux")), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true, true, true, TimeSeriesParams.MetricType.counter, Collections.emptyMap());
        builder.add("index2", false, true, true, true, TimeSeriesParams.MetricType.gauge, Map.of("foo", "bar"));
        builder.add("index3", false, true, true, true, TimeSeriesParams.MetricType.counter, Map.of("foo", "quux"));
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(true));
            assertThat(cap1.isDimension(), equalTo(true));
            assertNull(cap1.getMetricType());
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertNull(cap1.nonDimensionIndices());
            assertEquals(Map.of("foo", Set.of("bar", "quux")), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(true));
            assertThat(cap2.isDimension(), equalTo(true));
            assertNull(cap2.getMetricType());
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertNull(cap2.nonDimensionIndices());
            assertEquals(Map.of("foo", Set.of("bar", "quux")), cap2.meta());
        }
    }

    static FieldCapabilities randomFieldCaps(String fieldName) {
        String[] indices = null;
        if (randomBoolean()) {
            indices = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonSearchableIndices = null;
        if (randomBoolean()) {
            nonSearchableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonSearchableIndices.length; i++) {
                nonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonAggregatableIndices = null;
        if (randomBoolean()) {
            nonAggregatableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonAggregatableIndices.length; i++) {
                nonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }

        String[] nonDimensionIndices = null;
        if (randomBoolean()) {
            nonDimensionIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonDimensionIndices.length; i++) {
                nonDimensionIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }

        String[] metricConflictsIndices = null;
        if (randomBoolean()) {
            metricConflictsIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < metricConflictsIndices.length; i++) {
                metricConflictsIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }

        Map<String, Set<String>> meta = switch (randomInt(2)) {
            case 0 -> Collections.emptyMap();
            case 1 -> Map.of("foo", Set.of("bar"));
            default -> Map.of("foo", Set.of("bar", "baz"));
        };

        return new FieldCapabilities(
            fieldName,
            randomAlphaOfLengthBetween(5, 20),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomFrom(TimeSeriesParams.MetricType.values()),
            indices,
            nonSearchableIndices,
            nonAggregatableIndices,
            nonDimensionIndices,
            metricConflictsIndices,
            meta
        );
    }

    @Override
    protected FieldCapabilities mutateInstance(FieldCapabilities instance) {
        String name = instance.getName();
        String type = instance.getType();
        boolean isMetadataField = instance.isMetadataField();
        boolean isSearchable = instance.isSearchable();
        boolean isAggregatable = instance.isAggregatable();
        boolean isDimension = instance.isDimension();
        TimeSeriesParams.MetricType metricType = instance.getMetricType();
        String[] indices = instance.indices();
        String[] nonSearchableIndices = instance.nonSearchableIndices();
        String[] nonAggregatableIndices = instance.nonAggregatableIndices();
        String[] nonDimensionIndices = instance.nonDimensionIndices();
        String[] metricConflictsIndices = instance.metricConflictsIndices();
        Map<String, Set<String>> meta = instance.meta();
        switch (between(0, 12)) {
            case 0:
                name += randomAlphaOfLengthBetween(1, 10);
                break;
            case 1:
                type += randomAlphaOfLengthBetween(1, 10);
                break;
            case 2:
                isSearchable = isSearchable == false;
                break;
            case 3:
                isAggregatable = isAggregatable == false;
                break;
            case 4:
                String[] newIndices;
                int startIndicesPos = 0;
                if (indices == null) {
                    newIndices = new String[between(1, 10)];
                } else {
                    newIndices = Arrays.copyOf(indices, indices.length + between(1, 10));
                    startIndicesPos = indices.length;
                }
                for (int i = startIndicesPos; i < newIndices.length; i++) {
                    newIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                indices = newIndices;
                break;
            case 5:
                String[] newNonSearchableIndices;
                int startNonSearchablePos = 0;
                if (nonSearchableIndices == null) {
                    newNonSearchableIndices = new String[between(1, 10)];
                } else {
                    newNonSearchableIndices = Arrays.copyOf(nonSearchableIndices, nonSearchableIndices.length + between(1, 10));
                    startNonSearchablePos = nonSearchableIndices.length;
                }
                for (int i = startNonSearchablePos; i < newNonSearchableIndices.length; i++) {
                    newNonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                nonSearchableIndices = newNonSearchableIndices;
                break;
            case 6:
                String[] newNonAggregatableIndices;
                int startNonAggregatablePos = 0;
                if (nonAggregatableIndices == null) {
                    newNonAggregatableIndices = new String[between(1, 10)];
                } else {
                    newNonAggregatableIndices = Arrays.copyOf(nonAggregatableIndices, nonAggregatableIndices.length + between(1, 10));
                    startNonAggregatablePos = nonAggregatableIndices.length;
                }
                for (int i = startNonAggregatablePos; i < newNonAggregatableIndices.length; i++) {
                    newNonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                nonAggregatableIndices = newNonAggregatableIndices;
                break;
            case 7:
                Map<String, Set<String>> newMeta;
                if (meta.isEmpty()) {
                    newMeta = Map.of("foo", Set.of("bar"));
                } else {
                    newMeta = Collections.emptyMap();
                }
                meta = newMeta;
                break;
            case 8:
                isMetadataField = isMetadataField == false;
                break;
            case 9:
                isDimension = isDimension == false;
                break;
            case 10:
                if (metricType == null) {
                    metricType = randomFrom(TimeSeriesParams.MetricType.values());
                } else {
                    if (randomBoolean()) {
                        metricType = null;
                    } else {
                        metricType = randomValueOtherThan(metricType, () -> randomFrom(TimeSeriesParams.MetricType.values()));
                    }
                }
                break;
            case 11:
                String[] newTimeSeriesDimensionsConflictsIndices;
                int startTimeSeriesDimensionsConflictsPos = 0;
                if (nonDimensionIndices == null) {
                    newTimeSeriesDimensionsConflictsIndices = new String[between(1, 10)];
                } else {
                    newTimeSeriesDimensionsConflictsIndices = Arrays.copyOf(
                        nonDimensionIndices,
                        nonDimensionIndices.length + between(1, 10)
                    );
                    startTimeSeriesDimensionsConflictsPos = nonDimensionIndices.length;
                }
                for (int i = startTimeSeriesDimensionsConflictsPos; i < newTimeSeriesDimensionsConflictsIndices.length; i++) {
                    newTimeSeriesDimensionsConflictsIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                nonDimensionIndices = newTimeSeriesDimensionsConflictsIndices;
                break;
            case 12:
                String[] newMetricConflictsIndices;
                int startMetricConflictsPos = 0;
                if (metricConflictsIndices == null) {
                    newMetricConflictsIndices = new String[between(1, 10)];
                } else {
                    newMetricConflictsIndices = Arrays.copyOf(metricConflictsIndices, metricConflictsIndices.length + between(1, 10));
                    startMetricConflictsPos = metricConflictsIndices.length;
                }
                for (int i = startMetricConflictsPos; i < newMetricConflictsIndices.length; i++) {
                    newMetricConflictsIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                metricConflictsIndices = newMetricConflictsIndices;
                break;
            default:
                throw new AssertionError();
        }
        return new FieldCapabilities(
            name,
            type,
            isMetadataField,
            isSearchable,
            isAggregatable,
            isDimension,
            metricType,
            indices,
            nonSearchableIndices,
            nonAggregatableIndices,
            nonDimensionIndices,
            metricConflictsIndices,
            meta
        );
    }
}
