/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

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
        registerScriptChecks(checker);
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

    public void testIPv6WithMaxHextets() throws Exception {
        // IPv6 addresses starting with "::" followed by exactly 7 hextets.
        // These are valid addresses that should be indexed correctly.
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        final String[] ipAddresses = {
            "::1:2:3:4:5:6:7",
            "::ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "::1:0:0:0:0:0:1",
            "::0:0:0:0:0:0:1",
            "::1:1:1:1:1:1:1" };

        for (final String ipAddress : ipAddresses) {
            final ParsedDocument doc = mapper.parse(source(b -> b.field("field", ipAddress)));

            final List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.size());
            final IndexableField pointField = fields.get(0);
            assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ipAddress))), pointField.binaryValue());
            final IndexableField dvField = fields.get(1);
            assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ipAddress))), dvField.binaryValue());
        }
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
            () -> createDocumentMapper(IndexVersion.current(), fieldMapping(b -> {
                b.field("type", "ip");
                b.field("null_value", ":1");
            }))
        );
        assertEquals(
            e.getMessage(),
            "Failed to parse mapping: Error parsing [null_value] on field [field]: ':1' is not an IP string literal."
        );

        createDocumentMapper(IndexVersions.V_7_9_0, fieldMapping(b -> {
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

        assertTimeSeriesIndexing();
    }

    public void testDimensionIndexedAndDocvalues() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", false);
            })));
            assertThat(e.getCause().getMessage(), containsString("Field [time_series_dimension] requires that [doc_values] is true"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", true).field("doc_values", false);
            })));
            assertThat(e.getCause().getMessage(), containsString("Field [time_series_dimension] requires that [doc_values] is true"));
        }
    }

    public void testDimensionMultiValuedFieldTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), IndexMode.TIME_SERIES);

        ParsedDocument doc = mapper.parse(source(null, b -> {
            b.array("field", "192.168.1.1", "192.168.1.1");
            b.field("@timestamp", Instant.now());
        }, TimeSeriesRoutingHashFieldMapper.encode(randomInt())));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testDimensionMultiValuedFieldNonTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.array("field", "192.168.1.1", "192.168.1.1");
            b.field("@timestamp", Instant.now());
        }));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
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
                .sorted(BytesRef::compareTo)
                .map(v -> InetAddressPoint.decode(v.bytes))
                .map(NetworkAddress::format)
                .collect(Collectors.toCollection(ArrayList::new));
            values.stream()
                .filter(v -> false == v.v2() instanceof InetAddress)
                .map(Tuple::v2)
                .sorted(SyntheticSourceMalformedValueSorter.comparator())
                .forEach(outList::add);
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
            if (FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled() && randomBoolean()) {
                b.startObject("doc_values");
                b.field("cardinality", ESTestCase.randomFrom("low", "high"));
                b.endObject();
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }

    @Override
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

    public void testMultiValueSortedSet() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "ip").startObject("doc_values").field("multi_value", "sorted_set").endObject())
        );
        IpFieldMapper mapper = (IpFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(
                new FieldMapper.DocValuesParameter.Values(
                    true,
                    FieldMapper.DocValuesParameter.Values.Cardinality.LOW,
                    FieldMapper.DocValuesParameter.Values.MultiValue.SORTED_SET
                )
            )
        );
    }

    /**
     * IP high-cardinality doc values have used the SeparateCount format since their introduction. This test pins that contract for
     * the current index version.
     */
    public void testHighCardinalityDocValuesUsesSeparateCountFormat() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "ip").startObject("doc_values").field("cardinality", "high").endObject())
        );

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        assertFalse(
            "primary IP high-cardinality doc_values must be written in SeparateCount format (with .counts companion) for the "
                + "current index version",
            doc.rootDoc().getFields("field.counts").isEmpty()
        );
    }

    /**
     * IP high-cardinality doc values are written via a MultiValuedBinaryDocValuesField. The feature was introduced after the
     * SeparateCount format became the norm, so there is no legacy IntegratedCount data. The primary write path must produce
     * SeparateCount output regardless of indexCreatedVersion so the read path can decode it.
     */
    public void testHighCardinalityDocValuesUsesSeparateCountFormatForPreviousIndexVersion() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        IndexVersion legacyVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);
        DocumentMapper mapper = createMapperService(
            legacyVersion,
            fieldMapping(b -> b.field("type", "ip").startObject("doc_values").field("cardinality", "high").endObject())
        ).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        assertFalse(
            "primary IP high-cardinality doc_values must be written in SeparateCount format (with .counts companion) even for "
                + "legacy index versions",
            doc.rootDoc().getFields("field.counts").isEmpty()
        );
    }

    public void testMultiValueDefaultIsSortedSet() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "ip")));
        IpFieldMapper mapper = (IpFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().multiValue(), equalTo(FieldMapper.DocValuesParameter.Values.MultiValue.SORTED_SET));
    }

    public void testMultiValueSortedNotAllowed() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "ip").startObject("doc_values").field("multi_value", "sorted").endObject())
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Unknown value [sorted] for field [multi_value] - accepted values are [no, sorted_set, arrays]")
        );
    }

    public void testMultiValueNoAcceptsSingleValue() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "ip").startObject("doc_values").field("multi_value", "no").endObject())
        );
        mapper.parse(source(b -> b.field("field", "::1")));
    }

    public void testMultiValueNoRejectsArray() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "ip").startObject("doc_values").field("multi_value", "no").endObject())
        );
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", "::1", "::2")))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("configured with [multi_value=no] but encountered multiple values in the same document")
        );
    }

    /**
     * Single malformed value routes to the {@code _ignored} fallback. Only one {@link FieldMapper#parse(DocumentParserContext)} call
     * fires, so enforcement does not trip.
     */
    public void testMultiValueNoAcceptsSingleIgnoreMalformedValue() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip").field("ignore_malformed", true);
            b.startObject("doc_values").field("multi_value", "no").endObject();
        }));
        mapper.parse(source(b -> b.field("field", "not-an-ip")));
    }

    /**
     * Both values are malformed and would route to the {@code _ignored} fallback. Enforcement still throws on the second
     * {@link FieldMapper#parse(DocumentParserContext)} call before either is handled.
     */
    public void testMultiValueNoRejectsTwoIgnoreMalformedFallbacks() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip").field("ignore_malformed", true);
            b.startObject("doc_values").field("multi_value", "no").endObject();
        }));
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", "not-an-ip", "also-not-an-ip")))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("configured with [multi_value=no] but encountered multiple values in the same document")
        );
    }

    public void testMultiValueNoRejectsNormalPlusIgnoreMalformedFallback() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip").field("ignore_malformed", true);
            b.startObject("doc_values").field("multi_value", "no").endObject();
        }));
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", "::1", "not-an-ip")))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("configured with [multi_value=no] but encountered multiple values in the same document")
        );
    }

    /**
     * Mirror of {@link #testMultiValueNoRejectsNormalPlusIgnoreMalformedFallback} with the order reversed: first value is malformed
     * and would route to the {@code _ignored} fallback, second is a valid IP. Enforcement still fires on the second parse call.
     */
    public void testMultiValueNoRejectsIgnoreMalformedFallbackPlusNormal() throws IOException {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip").field("ignore_malformed", true);
            b.startObject("doc_values").field("multi_value", "no").endObject();
        }));
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", "not-an-ip", "::1")))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("configured with [multi_value=no] but encountered multiple values in the same document")
        );
    }

    @Override
    protected String randomSyntheticSourceKeep() {
        return "all";
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of(
            // TODO - shortcuts are disabled here, can we enable them?
            new SortShortcutSupport(this::minimalMapping, this::writeField, false),
            new SortShortcutSupport(IndexVersion.fromId(5000099), this::minimalMapping, this::writeField, false)
        );
    }
}
