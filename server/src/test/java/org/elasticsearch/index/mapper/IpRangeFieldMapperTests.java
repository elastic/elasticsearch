/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class IpRangeFieldMapperTests extends RangeFieldMapperTests {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "ip_range");
    }

    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return in.startObject("field").field("gt", "::ffff:c0a8:107").field("lt", "2001:db8::").endObject();
    }

    @Override
    protected String storedValue() {
        return InetAddresses.toAddrString(InetAddresses.forString("192.168.1.7"))
            + " : "
            + InetAddresses.toAddrString(InetAddresses.forString("2001:db8:0:0:0:0:0:0"));
    }

    @Override
    protected boolean supportsCoerce() {
        return false;
    }

    @Override
    protected Object rangeValue() {
        return "192.168.1.7";
    }

    @Override
    protected boolean supportsDecimalCoerce() {
        return false;
    }

    public void testStoreCidr() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "ip_range").field("store", true)));

        final Map<String, String> cases = new HashMap<>();
        cases.put("192.168.0.0/15", "192.169.255.255");
        cases.put("192.168.0.0/16", "192.168.255.255");
        cases.put("192.168.0.0/17", "192.168.127.255");
        for (final Map.Entry<String, String> entry : cases.entrySet()) {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", entry.getKey())));
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(3, fields.size());
            IndexableField dvField = fields.get(0);
            assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
            IndexableField pointField = fields.get(1);
            assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
            IndexableField storedField = fields.get(2);
            assertTrue(storedField.fieldType().stored());
            String strVal = InetAddresses.toAddrString(InetAddresses.forString("192.168.0.0"))
                + " : "
                + InetAddresses.toAddrString(InetAddresses.forString(entry.getValue()));
            assertThat(storedField.stringValue(), containsString(strVal));
        }
    }

    @SuppressWarnings("unchecked")
    public void testValidSyntheticSource() throws IOException {
        CheckedConsumer<XContentBuilder, IOException> mapping = b -> {
            b.startObject("field");
            b.field("type", "ip_range");
            if (rarely()) {
                b.field("index", false);
            }
            if (rarely()) {
                b.field("store", false);
            }
            b.endObject();
        };

        var values = randomList(1, 5, this::generateValue);
        var inputValues = values.stream().map(Tuple::v1).toList();
        var expectedValues = values.stream().map(Tuple::v2).toList();

        var source = getSourceFor(mapping, inputValues);

        // This is the main reason why we need custom logic.
        // IP ranges are serialized into binary doc values in unpredictable order
        // because API uses a set.
        // So this assert needs to be not sensitive to order and in "reference"
        // implementation of tests from MapperTestCase it is.
        var actual = source.source().get("field");
        var expected = new HashSet<>(expectedValues);
        if (expected.size() == 1) {
            assertEquals(expectedValues.get(0), actual);
        } else {
            assertThat(actual, instanceOf(List.class));
            assertTrue(((List<Object>) actual).containsAll(expected));
        }
    }

    private Tuple<Object, Map<String, Object>> generateValue() {
        String cidr = randomCidrBlock();
        InetAddresses.IpRange range = InetAddresses.parseIpRangeFromCidr(cidr);

        var includeFrom = randomBoolean();
        var includeTo = randomBoolean();

        Object input;
        // "to" field always comes first.
        Map<String, Object> output = new LinkedHashMap<>();
        if (randomBoolean()) {
            // CIDRs are always inclusive ranges.
            input = cidr;

            var from = InetAddresses.toAddrString(range.lowerBound());
            inclusiveFrom(output, from);

            var to = InetAddresses.toAddrString(range.upperBound());
            inclusiveTo(output, to);
        } else {
            var fromKey = includeFrom ? "gte" : "gt";
            var toKey = includeTo ? "lte" : "lt";
            var from = rarely() ? null : InetAddresses.toAddrString(range.lowerBound());
            var to = rarely() ? null : InetAddresses.toAddrString(range.upperBound());
            input = (ToXContent) (builder, params) -> {
                builder.startObject();
                if (includeFrom && from == null && randomBoolean()) {
                    // skip field entirely since it is equivalent to a default value
                } else {
                    builder.field(fromKey, from);
                }

                if (includeTo && to == null && randomBoolean()) {
                    // skip field entirely since it is equivalent to a default value
                } else {
                    builder.field(toKey, to);
                }

                return builder.endObject();
            };

            if (includeFrom) {
                inclusiveFrom(output, from);
            } else {
                var fromWithDefaults = from != null ? range.lowerBound() : (InetAddress) rangeType().minValue();
                var adjustedFrom = (InetAddress) rangeType().nextUp(fromWithDefaults);
                output.put("gte", InetAddresses.toAddrString(adjustedFrom));
            }

            if (includeTo) {
                inclusiveTo(output, to);
            } else {
                var toWithDefaults = to != null ? range.upperBound() : (InetAddress) rangeType().maxValue();
                var adjustedTo = (InetAddress) rangeType().nextDown(toWithDefaults);
                output.put("lte", InetAddresses.toAddrString(adjustedTo));
            }
        }

        return Tuple.tuple(input, output);
    }

    private void inclusiveFrom(Map<String, Object> output, String from) {
        // This is helpful since different representations can map to "::"
        var normalizedMin = InetAddresses.toAddrString((InetAddress) rangeType().minValue());
        if (from != null && from.equals(normalizedMin) == false) {
            output.put("gte", from);
        } else {
            output.put("gte", null);
        }
    }

    private void inclusiveTo(Map<String, Object> output, String to) {
        var normalizedMax = InetAddresses.toAddrString((InetAddress) rangeType().maxValue());
        if (to != null && to.equals(normalizedMax) == false) {
            output.put("lte", to);
        } else {
            output.put("lte", null);
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("custom version of synthetic source tests is implemented");
    }

    private static String randomCidrBlock() {
        boolean ipv4 = randomBoolean();

        InetAddress address = randomIp(ipv4);
        // exclude smallest prefix lengths to avoid empty ranges
        int prefixLength = ipv4 ? randomIntBetween(0, 30) : randomIntBetween(0, 126);

        return InetAddresses.toCidrString(address, prefixLength);
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.IP;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
