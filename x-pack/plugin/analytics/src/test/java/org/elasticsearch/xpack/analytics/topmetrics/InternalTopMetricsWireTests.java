/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public class InternalTopMetricsWireTests extends AbstractWireSerializingTestCase<InternalTopMetrics> {
    private static final List<DocValueFormat> RANDOM_FORMATS = unmodifiableList(Arrays.asList(
            DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN
    ));

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        for (DocValueFormat format : RANDOM_FORMATS) {
            writeables.add(new NamedWriteableRegistry.Entry(DocValueFormat.class, format.getWriteableName(), in -> format));
        }
        writeables.addAll(SortValue.namedWriteables());
        return new NamedWriteableRegistry(writeables); 
    }

    @Override
    protected InternalTopMetrics createTestInstance() {
        String name = randomAlphaOfLength(5);
        DocValueFormat sortFormat = randomFrom(RANDOM_FORMATS);
        SortOrder sortOrder = randomFrom(SortOrder.values());
        SortValue sortValue = randomSortValue();
        String metricName = randomAlphaOfLength(5);
        double metricValue = randomDouble();
        return new InternalTopMetrics(name, sortFormat, sortOrder, sortValue, metricName, metricValue, emptyList(), null);
    }

    @Override
    protected InternalTopMetrics mutateInstance(InternalTopMetrics instance) throws IOException {
        String name = instance.getName();
        DocValueFormat sortFormat = instance.getSortFormat();
        SortOrder sortOrder = instance.getSortOrder();
        SortValue sortValue = instance.getSortValue();
        String metricName = instance.getMetricName();
        double metricValue = instance.getMetricValue();
        switch (randomInt(5)) {
        case 0:
            name = randomAlphaOfLength(6);
            break;
        case 1:
            sortFormat = randomValueOtherThan(sortFormat, () -> randomFrom(RANDOM_FORMATS));
            break;
        case 2:
            sortOrder = sortOrder == SortOrder.ASC ? SortOrder.DESC : SortOrder.ASC;
            break;
        case 3:
            sortValue = randomValueOtherThan(sortValue, InternalTopMetricsWireTests::randomSortValue);
            break;
        case 4:
            metricName = randomAlphaOfLength(6);
            break;
        case 5:
            metricValue = randomValueOtherThan(metricValue, () -> randomDouble());
            break;
        default:
            throw new IllegalArgumentException("bad mutation");
        }
        return new InternalTopMetrics(name, sortFormat, sortOrder, sortValue, metricName, metricValue, emptyList(), null);
    }

    @Override
    protected Reader<InternalTopMetrics> instanceReader() {
        return InternalTopMetrics::new;
    }

    private static SortValue randomSortValue() {
        return randomBoolean() ? SortValue.from(randomLong()) : SortValue.from(randomDouble());
    }
}
