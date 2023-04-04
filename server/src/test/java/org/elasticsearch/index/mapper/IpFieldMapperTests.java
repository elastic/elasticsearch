/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class IpFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return "::1";
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "ip");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "::1"));
        registerDimensionChecks(checker);
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(16, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
        IndexableField dvField = fields.get(1);
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), dvField.binaryValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("index", false);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField dvField = fields.get(0);
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("doc_values", false);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());

        fields = doc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(1, fields.size());
        assertEquals("field", fields.get(0).stringValue());

        FieldMapper m = (FieldMapper) mapper.mappers().getMapper("field");
        Query existsQuery = m.fieldType().existsQuery(null);
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), existsQuery);
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("store", true);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        IndexableField dvField = fields.get(1);
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        IndexableField storedField = fields.get(2);
        assertTrue(storedField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddress.getByName("::1"))), storedField.binaryValue());
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(exampleMalformedValue(":1").errorMatches("':1' is not an IP string literal"));
    }

    public void testNullValue() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getFields("field"), empty());

        mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("null_value", "::1");
        }));

        doc = mapper.parse(source(b -> b.nullField("field")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(16, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
        IndexableField dvField = fields.get(1);
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), dvField.binaryValue());
        assertFalse(dvField.fieldType().stored());

        mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.nullField("null_value");
        }));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getFields("field"), empty());

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(Version.CURRENT, fieldMapping(b -> {
                b.field("type", "ip");
                b.field("null_value", ":1");
            }))
        );
        assertEquals(
            e.getMessage(),
            "Failed to parse mapping: Error parsing [null_value] on field [field]: ':1' is not an IP string literal."
        );

        createDocumentMapper(Version.V_7_9_0, fieldMapping(b -> {
            b.field("type", "ip");
            b.field("null_value", ":1");
        }));
        assertWarnings("Error parsing [:1] as IP in [null_value] on field [field]); [null_value] will be ignored");
    }

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        IpFieldMapper.IpFieldType ft = (IpFieldMapper.IpFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        assertDimension(true, IpFieldMapper.IpFieldType::isDimension);
        assertDimension(false, IpFieldMapper.IpFieldType::isDimension);
    }

    public void testDimensionIndexedAndDocvalues() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", true).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", true);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
    }

    public void testDimensionMultiValuedField() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }));

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", "192.168.1.1", "192.168.1.1")))
        );
        assertThat(e.getCause().getMessage(), containsString("Dimension field [field] cannot be a multi-valued field"));
    }

    @Override
    protected String generateRandomInputValue(MappedFieldType ft) {
        return NetworkAddress.format(randomIp(randomBoolean()));
    }

    @Override
    protected boolean dedupAfterFetch() {
        return true;
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "ip");
                b.field("script", "test");
                b.field("null_value", 7);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "ip");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new IpSyntheticSourceSupport(ignoreMalformed);
    }

    private static class IpSyntheticSourceSupport implements SyntheticSourceSupport {
        private final InetAddress nullValue = usually() ? null : randomIp(randomBoolean());
        private final boolean ignoreMalformed;

        private IpSyntheticSourceSupport(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
        }

        @Override
        public SyntheticSourceExample example(int maxValues) {
            if (randomBoolean()) {
                Tuple<Object, Object> v = generateValue();
                if (v.v2() instanceof InetAddress a) {
                    return new SyntheticSourceExample(v.v1(), NetworkAddress.format(a), this::mapping);
                }
                return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
            }
            List<Tuple<Object, Object>> values = randomList(1, maxValues, this::generateValue);
            List<Object> in = values.stream().map(Tuple::v1).toList();
            List<Object> outList = values.stream()
                .filter(v -> v.v2() instanceof InetAddress)
                .map(v -> new BytesRef(InetAddressPoint.encode((InetAddress) v.v2())))
                .collect(Collectors.toSet())
                .stream()
                .sorted()
                .map(v -> InetAddressPoint.decode(v.bytes))
                .map(NetworkAddress::format)
                .collect(Collectors.toCollection(ArrayList::new));
            values.stream().filter(v -> false == v.v2() instanceof InetAddress).map(v -> v.v2()).forEach(outList::add);
            Object out = outList.size() == 1 ? outList.get(0) : outList;
            return new SyntheticSourceExample(in, out, this::mapping);
        }

        private Tuple<Object, Object> generateValue() {
            if (ignoreMalformed && randomBoolean()) {
                List<Supplier<Object>> choices = List.of(
                    () -> randomAlphaOfLength(3),
                    ESTestCase::randomInt,
                    ESTestCase::randomLong,
                    ESTestCase::randomFloat,
                    ESTestCase::randomDouble
                );
                Object v = randomFrom(choices).get();
                return Tuple.tuple(v, v);
            }
            if (nullValue != null && randomBoolean()) {
                return Tuple.tuple(null, nullValue);
            }
            InetAddress addr = randomIp(randomBoolean());
            return Tuple.tuple(NetworkAddress.format(addr), addr);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "ip");
            if (nullValue != null) {
                b.field("null_value", NetworkAddress.format(nullValue));
            }
            if (rarely()) {
                b.field("index", false);
            }
            if (rarely()) {
                b.field("store", false);
            }
            if (ignoreMalformed) {
                b.field("ignore_malformed", true);
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of(
                new SyntheticSourceInvalidExample(
                    equalTo("field [field] of type [ip] doesn't support synthetic source because it doesn't have doc values"),
                    b -> b.field("type", "ip").field("doc_values", false)
                )
            );
        }
    }

    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            protected IpFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new IpFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected IpFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new IpFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit("192.168.0.1");
                    }
                };
            }
        };
    }
}
