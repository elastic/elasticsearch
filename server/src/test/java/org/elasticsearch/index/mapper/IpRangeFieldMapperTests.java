/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
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
import static org.hamcrest.Matchers.equalTo;
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

        DocumentMapper mapper = createDocumentMapper(syntheticSourceMapping(mapping));

        var values = randomList(1, 5, this::generateValue);
        var inputValues = values.stream().map(Tuple::v1).toList();
        var expectedValues = values.stream().map(Tuple::v2).toList();

        CheckedConsumer<XContentBuilder, IOException> input = b -> {
            b.field("field");
            if (inputValues.size() == 1) {
                b.value(inputValues.get(0));
            } else {
                b.startArray();
                for (var range : inputValues) {
                    b.value(range);
                }
                b.endArray();
            }
        };

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapper.parse(source(input)).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SourceProvider provider = SourceProvider.fromSyntheticSource(mapper.mapping());
                Source synthetic = provider.getSource(getOnlyLeafReader(reader).getContext(), 0);

                // This is the main reason why we need custom logic.
                // IP ranges are serialized into binary doc values in unpredictable order
                // because API uses a set.
                // So this assert needs to be not sensitive to order and in "reference"
                // implementation of tests from MapperTestCase it is.
                var actual = synthetic.source().get("field");
                if (inputValues.size() == 1) {
                    assertEquals(expectedValues.get(0), actual);
                } else {
                    assertThat(actual, instanceOf(List.class));
                    assertEquals(new HashSet<>((List<Object>) actual), new HashSet<>(expectedValues));
                }
            }
        }
    }

    private Tuple<Object, Object> generateValue() {
        String cidr = randomCidrBlock();
        Tuple<InetAddress, InetAddress> range = InetAddresses.parseIpRangeFromCidr(cidr);

        var includeFrom = randomBoolean();
        var includeTo = randomBoolean();

        Object input;
        // "to" field always comes first.
        Map<String, Object> output = new LinkedHashMap<>();
        if (randomBoolean()) {
            // CIDRs are always inclusive ranges.
            input = cidr;
            output.put("gte", InetAddresses.toAddrString(range.v1()));
            output.put("lte", InetAddresses.toAddrString(range.v2()));
        } else {
            var fromKey = includeFrom ? "gte" : "gt";
            var toKey = includeTo ? "lte" : "lt";
            input = Map.of(fromKey, InetAddresses.toAddrString(range.v1()), toKey, InetAddresses.toAddrString(range.v2()));

            // When ranges are stored, they are always normalized to include both ends.
            // `includeFrom` and `includeTo` here refers to user input.
            var rawFrom = range.v1();
            var adjustedFrom = includeFrom ? rawFrom : (InetAddress) RangeType.IP.nextUp(rawFrom);
            output.put("gte", InetAddresses.toAddrString(adjustedFrom));
            var rawTo = range.v2();
            var adjustedTo = includeTo ? rawTo : (InetAddress) RangeType.IP.nextDown(rawTo);
            output.put("lte", InetAddresses.toAddrString(adjustedTo));
        }

        return Tuple.tuple(input, output);
    }

    public void testInvalidSyntheticSource() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(syntheticSourceMapping(b -> {
            b.startObject("field");
            b.field("type", "ip_range");
            b.field("doc_values", false);
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo("field [field] of type [ip_range] doesn't support synthetic source because it doesn't have doc values")
        );
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
