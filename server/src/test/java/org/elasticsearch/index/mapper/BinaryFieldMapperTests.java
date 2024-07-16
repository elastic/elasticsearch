/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BinaryFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        final byte[] binaryValue = new byte[100];
        binaryValue[56] = 1;
        return binaryValue;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "binary");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", true));
        checker.registerConflictCheck("store", b -> b.field("store", true));
    }

    public void testExistsQueryDocValuesEnabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", true);
            if (randomBoolean()) {
                b.field("store", randomBoolean());
            }
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

    public void testExistsQueryStoreEnabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
            if (randomBoolean()) {
                b.field("doc_values", false);
            }
        }));
        assertExistsQuery(mapperService);
    }

    public void testExistsQueryStoreAndDocValuesDiabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", false);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
    }

    public void testDefaultMapping() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");

        assertThat(mapper, instanceOf(BinaryFieldMapper.class));

        byte[] value = new byte[100];
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", value)));
        assertThat(doc.rootDoc().getFields("field"), empty());
    }

    public void testStoredValue() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", "true");
        }));

        // case 1: a simple binary value
        final byte[] binaryValue1 = new byte[100];
        binaryValue1[56] = 1;

        // case 2: a value that looks compressed: this used to fail in 1.x
        BytesStreamOutput out = new BytesStreamOutput();
        try (OutputStream compressed = CompressorFactory.COMPRESSOR.threadLocalOutputStream(out)) {
            new BytesArray(binaryValue1).writeTo(compressed);
        }
        final byte[] binaryValue2 = BytesReference.toBytes(out.bytes());
        assertTrue(CompressorFactory.isCompressed(new BytesArray(binaryValue2)));

        for (byte[] value : Arrays.asList(binaryValue1, binaryValue2)) {
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", value)));
            BytesRef indexedValue = doc.rootDoc().getBinaryValue("field");
            assertEquals(new BytesRef(value), indexedValue);
            IndexableField field = doc.rootDoc().getField("field");
            assertTrue(field.fieldType().stored());
            assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());

            MappedFieldType fieldType = mapperService.fieldType("field");
            Object originalValue = fieldType.valueForDisplay(indexedValue);
            assertEquals(new BytesArray(value), originalValue);
        }
    }

    public void testDefaultsForTimeSeriesIndex() throws IOException {
        var isStored = randomBoolean();

        var indexSettings = getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-08T23:40:53.384Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
            .build();

        var mapping = mapping(b -> {
            b.startObject("field");
            b.field("type", "binary");
            if (isStored) {
                b.field("store", "true");
            }
            b.endObject();

            b.startObject("@timestamp");
            b.field("type", "date");
            b.endObject();
            b.startObject("dimension");
            b.field("type", "keyword");
            b.field("time_series_dimension", "true");
            b.endObject();
        });
        DocumentMapper mapper = createMapperService(getVersion(), indexSettings, () -> true, mapping).documentMapper();

        var source = source(TimeSeriesRoutingHashFieldMapper.DUMMY_ENCODED_VALUE, b -> {
            b.field("field", Base64.getEncoder().encodeToString(randomByteArrayOfLength(10)));
            b.field("@timestamp", "2000-10-10T23:40:53.384Z");
            b.field("dimension", "dimension1");
        }, null);
        ParsedDocument doc = mapper.parse(source);

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        var docValuesField = fields.stream().filter(f -> f.fieldType().docValuesType() == DocValuesType.BINARY).findFirst();
        assertTrue("Doc values are not present", docValuesField.isPresent());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        if (rarely()) return null;
        byte[] value = randomByteArrayOfLength(randomIntBetween(1, 50));
        return Base64.getEncoder().encodeToString(value);
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", "binary").field("doc_values", true); // enable doc_values so the test is happy
    }

    @Override
    protected boolean dedupAfterFetch() {
        return true;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) throws IOException {
                if (randomBoolean()) {
                    var value = generateValue();
                    return new SyntheticSourceExample(value.v1(), value.v2(), this::mapping);
                }

                List<Tuple<String, byte[]>> values = randomList(1, maxValues, this::generateValue);

                var in = values.stream().map(Tuple::v1).toList();

                var outList = values.stream()
                    .map(Tuple::v2)
                    .map(BytesCompareUnsigned::new)
                    .collect(Collectors.toSet())
                    .stream()
                    .sorted()
                    .map(b -> encode(b.bytes))
                    .toList();
                Object out = outList.size() == 1 ? outList.get(0) : outList;

                return new SyntheticSourceExample(in, out, this::mapping);
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of(
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [binary] doesn't support synthetic source because it doesn't have doc values"),
                        b -> b.field("type", "binary")
                    ),
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [binary] doesn't support synthetic source because it doesn't have doc values"),
                        b -> b.field("type", "binary").field("doc_values", false)
                    )
                );
            }

            private Tuple<String, byte[]> generateValue() {
                var len = randomIntBetween(1, 256);
                var bytes = randomByteArrayOfLength(len);

                return Tuple.tuple(encode(bytes), bytes);
            }

            private String encode(byte[] bytes) {
                return Base64.getEncoder().encodeToString(bytes);
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", "binary").field("doc_values", "true");

                if (rarely()) {
                    b.field("store", false);
                }
            }

            private record BytesCompareUnsigned(byte[] bytes) implements Comparable<BytesCompareUnsigned> {
                @Override
                public int compareTo(BytesCompareUnsigned o) {
                    return Arrays.compareUnsigned(bytes, o.bytes);
                }
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
