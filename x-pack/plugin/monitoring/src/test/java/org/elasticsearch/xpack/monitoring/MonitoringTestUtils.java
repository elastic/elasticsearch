/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Random;

import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;

public final class MonitoringTestUtils {

    // maximum number of milliseconds before a five digit year comes in, which could change formatting
    public static  final long MAX_MILLIS_BEFORE_10000 = 253402300799999L;

    private MonitoringTestUtils() {
    }

    /**
     * Generates a random {@link MonitoringDoc.Node}
     */
    public static MonitoringDoc.Node randomMonitoringNode(final Random random) {
        final String id = RandomStrings.randomAsciiLettersOfLength(random, 5);
        final String name = RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10);

        final TransportAddress fakeTransportAddress = buildNewFakeTransportAddress();
        final String host = fakeTransportAddress.address().getHostString();
        final String transportAddress = fakeTransportAddress.toString();
        final String ip = fakeTransportAddress.getAddress();
        final long timestamp = RandomNumbers.randomLongBetween(random, 0, MAX_MILLIS_BEFORE_10000);

        return new MonitoringDoc.Node(id, host, transportAddress, ip, name, timestamp);
    }

    /**
     * Generates a random {@link MonitoringDoc}
     */
    public static MonitoringBulkDoc randomMonitoringBulkDoc(Random random) throws IOException {
        return randomMonitoringBulkDoc(random, RandomPicks.randomFrom(random, XContentType.values()));
    }

    /**
     * Generates a random {@link MonitoringDoc} with a given {@link XContentType}
     */
    public static MonitoringBulkDoc randomMonitoringBulkDoc(final Random random,
                                                            final XContentType xContentType) throws IOException {
        return randomMonitoringBulkDoc(random, xContentType, RandomObjects.randomSource(random, xContentType));
    }

    /**
     * Generates a random {@link MonitoringDoc} with a given {@link XContentType} and {@link BytesReference} source
     */
    public static MonitoringBulkDoc randomMonitoringBulkDoc(final Random random,
                                                            final XContentType xContentType,
                                                            final BytesReference source) throws IOException {
        return randomMonitoringBulkDoc(random, xContentType, source, RandomPicks.randomFrom(random, MonitoredSystem.values()));
    }

    /**
     * Generates a random {@link MonitoringDoc} with a given {@link XContentType}, {@link BytesReference} source and {@link MonitoredSystem}
     */
    public static MonitoringBulkDoc randomMonitoringBulkDoc(final Random random,
                                                            final XContentType xContentType,
                                                            final BytesReference source,
                                                            final MonitoredSystem system) throws IOException {
        final String type = RandomPicks.randomFrom(random, new String[]{"type1", "type2", "type3"});
        return randomMonitoringBulkDoc(random, xContentType, source, system, type);
    }

    /**
     * Generates a random {@link MonitoringDoc} with a given {@link XContentType}, {@link BytesReference} source,
     * {@link MonitoredSystem} and type.
     */
    public static MonitoringBulkDoc randomMonitoringBulkDoc(final Random random,
                                                            final XContentType xContentType,
                                                            final BytesReference source,
                                                            final MonitoredSystem system,
                                                            final String type) throws IOException {
        final String id = random.nextBoolean() ? RandomStrings.randomAsciiLettersOfLength(random, 5) : null;
        final long timestamp = RandomNumbers.randomLongBetween(random, 0L, MAX_MILLIS_BEFORE_10000);
        final long interval = RandomNumbers.randomLongBetween(random, 0L, Long.MAX_VALUE);
        return new MonitoringBulkDoc(system, type, id, timestamp, interval, source, xContentType);
    }
}
