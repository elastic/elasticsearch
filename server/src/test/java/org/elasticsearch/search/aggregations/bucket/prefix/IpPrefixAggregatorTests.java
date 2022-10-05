/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class IpPrefixAggregatorTests extends AggregatorTestCase {

    private static final Comparator<InternalIpPrefix.Bucket> IP_ADDRESS_KEY_COMPARATOR = Comparator.comparing(
        InternalIpPrefix.Bucket::getKeyAsString
    );

    private static final class TestIpDataHolder {
        private final String ipAddressAsString;
        private final InetAddress ipAddress;
        private final String subnetAsString;
        private final InetAddress subnet;
        private final int prefixLength;
        private final long time;

        TestIpDataHolder(final String ipAddressAsString, final String subnetAsString, final int prefixLength, final long time) {
            this.ipAddressAsString = ipAddressAsString;
            this.ipAddress = InetAddresses.forString(ipAddressAsString);
            this.subnetAsString = subnetAsString;
            this.subnet = InetAddresses.forString(subnetAsString);
            this.prefixLength = prefixLength;
            this.time = time;
        }

        public String getIpAddressAsString() {
            return ipAddressAsString;
        }

        public InetAddress getIpAddress() {
            return ipAddress;
        }

        public InetAddress getSubnet() {
            return subnet;
        }

        public String getSubnetAsString() {
            return subnetAsString;
        }

        public int getPrefixLength() {
            return prefixLength;
        }

        public long getTime() {
            return time;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestIpDataHolder that = (TestIpDataHolder) o;
            return prefixLength == that.prefixLength
                && time == that.time
                && Objects.equals(ipAddressAsString, that.ipAddressAsString)
                && Objects.equals(ipAddress, that.ipAddress)
                && Objects.equals(subnetAsString, that.subnetAsString)
                && Objects.equals(subnet, that.subnet);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ipAddressAsString, ipAddress, subnetAsString, subnet, prefixLength, time);
        }

        @Override
        public String toString() {
            return "TestIpDataHolder{"
                + "ipAddressAsString='"
                + ipAddressAsString
                + '\''
                + ", ipAddress="
                + ipAddress
                + ", subnetAsString='"
                + subnetAsString
                + '\''
                + ", subnet="
                + subnet
                + ", prefixLength="
                + prefixLength
                + ", time="
                + time
                + '}';
        }
    }

    public void testEmptyDocument() throws IOException {
        // GIVEN
        final int prefixLength = 16;
        final String field = "ipv4";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = Collections.emptyList();

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {

        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertTrue(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
        }, fieldType);
    }

    public void testIpv4Addresses() throws IOException {
        // GIVEN
        final int prefixLength = 16;
        final String field = "ipv4";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "192.169.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "10.19.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "10.122.0.0", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(1L, 1L, 4L, 1L)
            );
        }, fieldType);
    }

    public void testIpv6Addresses() throws IOException {
        // GIVEN
        final int prefixLength = 64;
        final String field = "ipv6";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(true)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("2001:db8:a4f8:112a:6001:0:12:7f2a", "2001:db8:a4f8:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4f8:112a:7044:1f01:0:44f2", "2001:db8:a4f8:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4ff:112a::7002:7ff2", "2001:db8:a4ff:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:1212:0:1:3", "3007:db81:4b11:234f::", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:7770:12f6:0:30", "3007:db81:4b11:234f::", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertTrue(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });
            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(2L, 1L, 2L)
            );
        }, fieldType);
    }

    public void testZeroPrefixLength() throws IOException {
        // GIVEN
        final int prefixLength = 0;
        final String field = "ipv4";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "0.0.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "0.0.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "0.0.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "0.0.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "0.0.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "0.0.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "0.0.0.0", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of((long) ipAddresses.size())
            );
        }, fieldType);
    }

    public void testIpv4MaxPrefixLength() throws IOException {
        // GIVEN
        final int prefixLength = 32;
        final String field = "ipv4";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.1.12", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "192.168.1.12", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "192.168.1.117", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "192.168.10.27", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "192.169.0.88", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "10.19.0.44", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "10.122.2.67", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(1L, 1L, 1L, 2L, 1L, 1L)
            );
        }, fieldType);
    }

    public void testIpv6MaxPrefixLength() throws IOException {
        // GIVEN
        final int prefixLength = 128;
        final String field = "ipv6";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(true)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("2001:db8:a4f8:112a:6001:0:12:7f2a", "2001:db8:a4f8:112a:6001:0:12:7f2a", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4f8:112a:7044:1f01:0:44f2", "2001:db8:a4f8:112a:7044:1f01:0:44f2", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4ff:112a:0:0:7002:7ff2", "2001:db8:a4ff:112a::7002:7ff2", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:1212:0:1:3", "3007:db81:4b11:234f:1212:0:1:3", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:7770:12f6:0:30", "3007:db81:4b11:234f:7770:12f6:0:30", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertTrue(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });
            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(1L, 1L, 1L, 1L, 1L)
            );
        }, fieldType);
    }

    public void testAggregateOnIpv4Field() throws IOException {
        // GIVEN
        final int prefixLength = 16;
        final String ipv4FieldName = "ipv4";
        final String ipv6FieldName = "ipv6";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(ipv4FieldName)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType[] fieldTypes = { new IpFieldMapper.IpFieldType(ipv4FieldName), new IpFieldMapper.IpFieldType(ipv6FieldName) };
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "192.169.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "10.19.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "10.122.0.0", prefixLength, defaultTime())
        );
        final String ipv6Value = "2001:db8:a4f8:112a:6001:0:12:7f2a";

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField(ipv4FieldName, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))),
                        new SortedDocValuesField(ipv6FieldName, new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ipv6Value))))
                    )
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(1L, 1L, 4L, 1L)
            );
        }, fieldTypes);
    }

    public void testAggregateOnIpv6Field() throws IOException {
        // GIVEN
        final int prefixLength = 64;
        final String ipv4FieldName = "ipv4";
        final String ipv6FieldName = "ipv6";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(ipv6FieldName)
            .isIpv6(true)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType[] fieldTypes = { new IpFieldMapper.IpFieldType(ipv4FieldName), new IpFieldMapper.IpFieldType(ipv6FieldName) };
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("2001:db8:a4f8:112a:6001:0:12:7f2a", "2001:db8:a4f8:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4f8:112a:7044:1f01:0:44f2", "2001:db8:a4f8:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4ff:112a::7002:7ff2", "2001:db8:a4ff:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:1212:0:1:3", "3007:db81:4b11:234f::", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:7770:12f6:0:30", "3007:db81:4b11:234f::", prefixLength, defaultTime())
        );
        final String ipv4Value = "192.168.10.20";

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField(ipv6FieldName, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))),
                        new SortedDocValuesField(ipv4FieldName, new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ipv4Value))))
                    )
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertTrue(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });
            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(2L, 1L, 2L)
            );
        }, fieldTypes);
    }

    public void testIpv4AggregationAsSubAggregation() throws IOException {
        // GIVEN
        final int prefixLength = 16;
        final String ipv4FieldName = "ipv4";
        final String datetimeFieldName = "datetime";
        final String dateHistogramAggregationName = "date_histogram";
        final String ipPrefixAggregationName = "ip_prefix";
        final AggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder(dateHistogramAggregationName).calendarInterval(
            DateHistogramInterval.DAY
        )
            .field(datetimeFieldName)
            .subAggregation(
                new IpPrefixAggregationBuilder(ipPrefixAggregationName).field(ipv4FieldName)
                    .isIpv6(false)
                    .keyed(randomBoolean())
                    .appendPrefixLength(false)
                    .minDocCount(1)
                    .prefixLength(prefixLength)
            );
        final DateFieldMapper.DateFieldType dateFieldType = new DateFieldMapper.DateFieldType(datetimeFieldName);
        final IpFieldMapper.IpFieldType ipFieldType = new IpFieldMapper.IpFieldType(ipv4FieldName);
        final MappedFieldType[] fieldTypes = { ipFieldType, dateFieldType };

        long day1 = dateFieldType.parse("2021-10-12");
        long day2 = dateFieldType.parse("2021-10-11");
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, day1),
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, day2),
            new TestIpDataHolder("192.168.1.117", "192.168.0.0", prefixLength, day1),
            new TestIpDataHolder("192.168.10.27", "192.168.0.0", prefixLength, day2),
            new TestIpDataHolder("192.169.0.88", "192.169.0.0", prefixLength, day1),
            new TestIpDataHolder("10.19.0.44", "10.19.0.0", prefixLength, day2),
            new TestIpDataHolder("10.122.2.67", "10.122.0.0", prefixLength, day1),
            new TestIpDataHolder("10.19.13.32", "10.19.0.0", prefixLength, day2)
        );

        final Set<String> expectedBucket1Subnets = ipAddresses.stream()
            .filter(testIpDataHolder -> testIpDataHolder.getTime() == day1)
            .map(TestIpDataHolder::getSubnetAsString)
            .collect(Collectors.toUnmodifiableSet());
        final Set<String> expectedBucket2Subnets = ipAddresses.stream()
            .filter(testIpDataHolder -> testIpDataHolder.getTime() == day2)
            .map(TestIpDataHolder::getSubnetAsString)
            .collect(Collectors.toUnmodifiableSet());

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (final TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField(ipv4FieldName, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))),
                        new SortedNumericDocValuesField(datetimeFieldName, ipDataHolder.getTime())
                    )
                );
            }
        }, agg -> {
            final InternalDateHistogram dateHistogram = (InternalDateHistogram) agg;
            final List<InternalDateHistogram.Bucket> buckets = dateHistogram.getBuckets();
            assertEquals(2, buckets.size());

            final InternalDateHistogram.Bucket day1Bucket = buckets.stream()
                .filter(bucket -> bucket.getKey().equals(Instant.ofEpochMilli(day1).atZone(ZoneOffset.UTC)))
                .findAny()
                .orElse(null);
            final InternalDateHistogram.Bucket day2Bucket = buckets.stream()
                .filter(bucket -> bucket.getKey().equals(Instant.ofEpochMilli(day2).atZone(ZoneOffset.UTC)))
                .findAny()
                .orElse(null);
            final InternalIpPrefix ipPrefix1 = Objects.requireNonNull(day1Bucket).getAggregations().get(ipPrefixAggregationName);
            final InternalIpPrefix ipPrefix2 = Objects.requireNonNull(day2Bucket).getAggregations().get(ipPrefixAggregationName);
            assertNotNull(ipPrefix1);
            assertNotNull(ipPrefix2);
            assertEquals(expectedBucket1Subnets.size(), ipPrefix1.getBuckets().size());
            assertEquals(expectedBucket2Subnets.size(), ipPrefix2.getBuckets().size());

            final Set<String> bucket1Subnets = ipPrefix1.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> bucket2Subnets = ipPrefix2.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());
            assertTrue(bucket1Subnets.containsAll(expectedBucket1Subnets));
            assertTrue(bucket2Subnets.containsAll(expectedBucket2Subnets));
            assertTrue(expectedBucket1Subnets.containsAll(bucket1Subnets));
            assertTrue(expectedBucket2Subnets.containsAll(bucket2Subnets));
        }, fieldTypes);
    }

    public void testIpv6AggregationAsSubAggregation() throws IOException {
        // GIVEN
        final int prefixLength = 64;
        final String ipv4FieldName = "ipv6";
        final String datetimeFieldName = "datetime";
        final String dateHistogramAggregationName = "date_histogram";
        final String ipPrefixAggregationName = "ip_prefix";
        final AggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder(dateHistogramAggregationName).calendarInterval(
            DateHistogramInterval.DAY
        )
            .field(datetimeFieldName)
            .subAggregation(
                new IpPrefixAggregationBuilder(ipPrefixAggregationName).field(ipv4FieldName)
                    .isIpv6(true)
                    .keyed(randomBoolean())
                    .appendPrefixLength(false)
                    .minDocCount(1)
                    .prefixLength(prefixLength)
            );
        final DateFieldMapper.DateFieldType dateFieldType = new DateFieldMapper.DateFieldType(datetimeFieldName);
        final IpFieldMapper.IpFieldType ipFieldType = new IpFieldMapper.IpFieldType(ipv4FieldName);
        final MappedFieldType[] fieldTypes = { ipFieldType, dateFieldType };

        long day1 = dateFieldType.parse("2021-11-04");
        long day2 = dateFieldType.parse("2021-11-05");
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("2001:db8:a4f8:112a:6001:0:12:7f2a", "2001:db8:a4f8:112a::", prefixLength, day1),
            new TestIpDataHolder("2001:db8:a4f8:112a:7044:1f01:0:44f2", "2001:db8:a4f8:112a::", prefixLength, day1),
            new TestIpDataHolder("2001:db8:a4ff:112a::7002:7ff2", "2001:db8:a4ff:112a::", prefixLength, day2),
            new TestIpDataHolder("3007:db81:4b11:234f:1212:0:1:3", "3007:db81:4b11:234f::", prefixLength, day2),
            new TestIpDataHolder("3007:db81:4b11:234f:7770:12f6:0:30", "3007:db81:4b11:234f::", prefixLength, day1)
        );

        final Set<String> expectedBucket1Subnets = ipAddresses.stream()
            .filter(testIpDataHolder -> testIpDataHolder.getTime() == day1)
            .map(TestIpDataHolder::getSubnetAsString)
            .collect(Collectors.toUnmodifiableSet());
        final Set<String> expectedBucket2Subnets = ipAddresses.stream()
            .filter(testIpDataHolder -> testIpDataHolder.getTime() == day2)
            .map(TestIpDataHolder::getSubnetAsString)
            .collect(Collectors.toUnmodifiableSet());

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (final TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField(ipv4FieldName, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))),
                        new SortedNumericDocValuesField(datetimeFieldName, ipDataHolder.getTime())
                    )
                );
            }
        }, agg -> {
            final InternalDateHistogram dateHistogram = (InternalDateHistogram) agg;
            final List<InternalDateHistogram.Bucket> buckets = dateHistogram.getBuckets();
            assertEquals(2, buckets.size());

            final InternalDateHistogram.Bucket day1Bucket = buckets.stream()
                .filter(bucket -> bucket.getKey().equals(Instant.ofEpochMilli(day1).atZone(ZoneOffset.UTC)))
                .findAny()
                .orElse(null);
            final InternalDateHistogram.Bucket day2Bucket = buckets.stream()
                .filter(bucket -> bucket.getKey().equals(Instant.ofEpochMilli(day2).atZone(ZoneOffset.UTC)))
                .findAny()
                .orElse(null);
            final InternalIpPrefix ipPrefix1 = Objects.requireNonNull(day1Bucket).getAggregations().get(ipPrefixAggregationName);
            final InternalIpPrefix ipPrefix2 = Objects.requireNonNull(day2Bucket).getAggregations().get(ipPrefixAggregationName);
            assertNotNull(ipPrefix1);
            assertNotNull(ipPrefix2);
            assertEquals(expectedBucket1Subnets.size(), ipPrefix1.getBuckets().size());
            assertEquals(expectedBucket2Subnets.size(), ipPrefix2.getBuckets().size());

            final Set<String> bucket1Subnets = ipPrefix1.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> bucket2Subnets = ipPrefix2.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());
            assertTrue(bucket1Subnets.containsAll(expectedBucket1Subnets));
            assertTrue(bucket2Subnets.containsAll(expectedBucket2Subnets));
            assertTrue(expectedBucket1Subnets.containsAll(bucket1Subnets));
            assertTrue(expectedBucket2Subnets.containsAll(bucket2Subnets));
        }, fieldTypes);
    }

    public void testIpPrefixSubAggregations() throws IOException {
        // GIVEN
        final int topPrefixLength = 16;
        final int subPrefixLength = 24;
        final String ipv4FieldName = "ipv4";
        final String topIpPrefixAggregation = "top_ip_prefix";
        final String subIpPrefixAggregation = "sub_ip_prefix";
        final AggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder(topIpPrefixAggregation).field(ipv4FieldName)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(topPrefixLength)
            .subAggregation(
                new IpPrefixAggregationBuilder(subIpPrefixAggregation).field(ipv4FieldName)
                    .isIpv6(false)
                    .keyed(randomBoolean())
                    .appendPrefixLength(false)
                    .minDocCount(1)
                    .prefixLength(subPrefixLength)
            );
        final IpFieldMapper.IpFieldType ipFieldType = new IpFieldMapper.IpFieldType(ipv4FieldName);
        final MappedFieldType[] fieldTypes = { ipFieldType };

        final String FIRST_SUBNET = "192.168.0.0";
        final String SECOND_SUBNET = "192.169.0.0";
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", FIRST_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.12", FIRST_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", FIRST_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.27", FIRST_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.169.1.18", SECOND_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.168.2.129", FIRST_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.169.2.49", SECOND_SUBNET, topPrefixLength, defaultTime()),
            new TestIpDataHolder("192.169.1.201", SECOND_SUBNET, topPrefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (final TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(new SortedDocValuesField(ipv4FieldName, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix topIpPrefix = (InternalIpPrefix) agg;
            final List<InternalIpPrefix.Bucket> buckets = topIpPrefix.getBuckets();
            assertEquals(2, buckets.size());

            final InternalIpPrefix.Bucket firstSubnetBucket = topIpPrefix.getBuckets()
                .stream()
                .filter(bucket -> FIRST_SUBNET.equals(bucket.getKeyAsString()))
                .findAny()
                .orElse(null);
            final InternalIpPrefix.Bucket secondSubnetBucket = topIpPrefix.getBuckets()
                .stream()
                .filter(bucket -> SECOND_SUBNET.equals(bucket.getKeyAsString()))
                .findAny()
                .orElse(null);
            assertNotNull(firstSubnetBucket);
            assertNotNull(secondSubnetBucket);
            assertEquals(5, firstSubnetBucket.getDocCount());
            assertEquals(3, secondSubnetBucket.getDocCount());

            final InternalIpPrefix firstBucketSubAggregation = firstSubnetBucket.getAggregations().get(subIpPrefixAggregation);
            final InternalIpPrefix secondBucketSubAggregation = secondSubnetBucket.getAggregations().get(subIpPrefixAggregation);
            final Set<String> firstSubnetNestedSubnets = firstBucketSubAggregation.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> secondSubnetNestedSubnets = secondBucketSubAggregation.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());
            final List<String> expectedFirstSubnetNestedSubnets = List.of("192.168.1.0", "192.168.2.0", "192.168.10.0");
            final List<String> expectedSecondSubnetNestedSUbnets = List.of("192.169.1.0", "192.169.2.0");
            assertTrue(firstSubnetNestedSubnets.containsAll(expectedFirstSubnetNestedSubnets));
            assertTrue(expectedFirstSubnetNestedSubnets.containsAll(firstSubnetNestedSubnets));
            assertTrue(secondSubnetNestedSubnets.containsAll(expectedSecondSubnetNestedSUbnets));
            assertTrue(expectedSecondSubnetNestedSUbnets.containsAll(secondSubnetNestedSubnets));

        }, fieldTypes);
    }

    public void testIpv4AppendPrefixLength() throws IOException {
        // GIVEN
        final int prefixLength = 16;
        final String field = "ipv4";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(true)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "192.169.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "10.19.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "10.122.0.0", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .map(appendPrefixLength(prefixLength))
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .map(appendPrefixLength(prefixLength))
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertTrue(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
        }, fieldType);
    }

    public void testIpv6AppendPrefixLength() throws IOException {
        // GIVEN
        final int prefixLength = 64;
        final String field = "ipv6";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(true)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("2001:db8:a4f8:112a:6001:0:12:7f2a", "2001:db8:a4f8:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4f8:112a:7044:1f01:0:44f2", "2001:db8:a4f8:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("2001:db8:a4ff:112a::7002:7ff2", "2001:db8:a4ff:112a::", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:1212:0:1:3", "3007:db81:4b11:234f::", prefixLength, defaultTime()),
            new TestIpDataHolder("3007:db81:4b11:234f:7770:12f6:0:30", "3007:db81:4b11:234f::", prefixLength, defaultTime())
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .map(appendPrefixLength(prefixLength))
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .map(appendPrefixLength(prefixLength))
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertTrue(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });
            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
        }, fieldType);
    }

    public void testMinDocCount() throws IOException {
        final int prefixLength = 16;
        final String field = "ipv4";
        int minDocCount = 2;
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(minDocCount)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "192.169.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "10.19.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "10.122.0.0", prefixLength, defaultTime())
        );

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    singleton(new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))))
                );
            }
        }, (InternalIpPrefix ipPrefix) -> {
            final Set<String> expectedSubnets = Set.of("192.168.0.0");
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertTrue(
                ipPrefix.getBuckets().stream().map(InternalIpPrefix.Bucket::getDocCount).allMatch(docCount -> docCount >= minDocCount)
            );
            assertThat(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                equalTo(List.of(4L))
            );
        }, fieldType);
    }

    public void testAggregationWithQueryFilter() throws IOException {
        // GIVEN
        final int prefixLength = 16;
        final String field = "ipv4";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder("ip_prefix").field(field)
            .isIpv6(false)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength);
        final MappedFieldType fieldType = new IpFieldMapper.IpFieldType(field);
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.12", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.1.117", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.168.10.27", "192.168.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("192.169.0.88", "192.169.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.19.0.44", "10.19.0.0", prefixLength, defaultTime()),
            new TestIpDataHolder("10.122.2.67", "10.122.0.0", prefixLength, defaultTime())
        );
        final Query query = InetAddressPoint.newRangeQuery(
            field,
            InetAddresses.forString("192.168.0.0"),
            InetAddressPoint.nextDown(InetAddresses.forString("192.169.0.0"))
        );

        // WHEN
        testCase(aggregationBuilder, query, iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField(field, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))),
                        new InetAddressPoint(field, ipDataHolder.getIpAddress())
                    )
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .filter(subnet -> subnet.startsWith("192.168."))
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertFalse(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });

            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));
            assertEquals(
                ipPrefix.getBuckets().stream().sorted(IP_ADDRESS_KEY_COMPARATOR).map(InternalIpPrefix.Bucket::getDocCount).toList(),
                List.of(4L)
            );
        }, fieldType);
    }

    public void testMetricAggregation() throws IOException {
        // GIVEN
        final int prefixLength = 64;
        final String ipField = "ipv6";
        final String timeField = "time";
        final String topAggregationName = "ip_prefix";
        final String subAggregationName = "total_time";
        final IpPrefixAggregationBuilder aggregationBuilder = new IpPrefixAggregationBuilder(topAggregationName).field(ipField)
            .isIpv6(true)
            .keyed(randomBoolean())
            .appendPrefixLength(false)
            .minDocCount(1)
            .prefixLength(prefixLength)
            .subAggregation(new SumAggregationBuilder(subAggregationName).field(timeField));
        final MappedFieldType[] fieldTypes = {
            new IpFieldMapper.IpFieldType(ipField),
            new NumberFieldMapper.NumberFieldType(timeField, NumberFieldMapper.NumberType.LONG) };
        final List<TestIpDataHolder> ipAddresses = List.of(
            new TestIpDataHolder("2001:db8:a4f8:112a:6001:0:12:7f2a", "2001:db8:a4f8:112a::", prefixLength, 100),
            new TestIpDataHolder("2001:db8:a4f8:112a:7044:1f01:0:44f2", "2001:db8:a4f8:112a::", prefixLength, 110),
            new TestIpDataHolder("2001:db8:a4ff:112a::7002:7ff2", "2001:db8:a4ff:112a::", prefixLength, 200),
            new TestIpDataHolder("3007:db81:4b11:234f:1212:0:1:3", "3007:db81:4b11:234f::", prefixLength, 170),
            new TestIpDataHolder("3007:db81:4b11:234f:7770:12f6:0:30", "3007:db81:4b11:234f::", prefixLength, 130)
        );

        // WHEN
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (TestIpDataHolder ipDataHolder : ipAddresses) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField(ipField, new BytesRef(InetAddressPoint.encode(ipDataHolder.getIpAddress()))),
                        new NumericDocValuesField(timeField, ipDataHolder.getTime())
                    )
                );
            }
        }, agg -> {
            final InternalIpPrefix ipPrefix = (InternalIpPrefix) agg;
            final Set<String> expectedSubnets = ipAddresses.stream()
                .map(TestIpDataHolder::getSubnetAsString)
                .collect(Collectors.toUnmodifiableSet());
            final Set<String> ipAddressesAsString = ipPrefix.getBuckets()
                .stream()
                .map(InternalIpPrefix.Bucket::getKeyAsString)
                .collect(Collectors.toUnmodifiableSet());

            // THEN
            ipPrefix.getBuckets().forEach(bucket -> {
                assertTrue(bucket.isIpv6());
                assertFalse(bucket.appendPrefixLength());
                assertEquals(prefixLength, bucket.getPrefixLength());
            });
            assertFalse(ipPrefix.getBuckets().isEmpty());
            assertEquals(expectedSubnets.size(), ipPrefix.getBuckets().size());
            assertTrue(ipAddressesAsString.containsAll(expectedSubnets));
            assertTrue(expectedSubnets.containsAll(ipAddressesAsString));

            assertEquals(210, ((Sum) ipPrefix.getBuckets().get(0).getAggregations().get(subAggregationName)).value(), 0);
            assertEquals(200, ((Sum) ipPrefix.getBuckets().get(1).getAggregations().get(subAggregationName)).value(), 0);
            assertEquals(300, ((Sum) ipPrefix.getBuckets().get(2).getAggregations().get(subAggregationName)).value(), 0);
        }, fieldTypes);
    }

    private Function<String, String> appendPrefixLength(int prefixLength) {
        return subnetAddress -> subnetAddress + "/" + prefixLength;
    }

    private long defaultTime() {
        return randomLongBetween(0, Long.MAX_VALUE);
    }
}
