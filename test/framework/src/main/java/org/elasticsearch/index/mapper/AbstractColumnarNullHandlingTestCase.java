/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.ElasticsearchStoredFieldsFormat;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Shared coverage for how the string mappers record a {@code null} under the ColumNAR codec, where a field's doc values are a payload
 * carrying their own slot count.
 *
 * <p>Because that payload is added to the document as soon as it exists, a document whose slots are all null would otherwise carry the
 * field as doc values alone, with no index options, while a document holding a value carries it as both doc values and postings. Lucene
 * builds a field's {@code FieldInfo} from the first document that has it and rejects any later document that presents the field with
 * different index options, so the two shapes have to agree. The agreed behaviour is:
 *
 * <ul>
 *   <li>a bare {@code null} is dropped outright: it reaches neither the postings nor the doc values, so the document does not carry
 *       the field at all;</li>
 *   <li>a null inside an array keeps its slot in the doc values, so synthetic source can put the null back in its original position,
 *       and if the document ends up holding no value the payload reports the field's index options itself, inverting into nothing so
 *       that it adds no term a query could match.</li>
 * </ul>
 *
 * <p>This applies only under the codec. The layouts a strictly columnar index uses without it keep a document's slot count in a
 * companion column and so never register the field for an all-null document; their behaviour is unchanged and is covered by
 * {@link AbstractColumnarArrayOrderFieldDataTestCase}.
 *
 * <p>Concrete subclasses supply the field type (keyword, text, match_only_text).
 */
public abstract class AbstractColumnarNullHandlingTestCase extends MapperServiceTestCase {

    protected static final String FIELD = "field";

    protected abstract String fieldTypeName();

    /** A value of the field's type that indexes to at least one term, used as the document the all-null document has to agree with. */
    protected String sampleValue() {
        return "a value";
    }

    @Before
    public void assumeColumnarCodecEnabled() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
    }

    /**
     * A columnar index with the codec on and the field indexed. {@code index} is set explicitly because a strictly columnar index
     * defaults it to {@code false} for keyword (see {@code IndexSettings#INDEX_DISABLED_BY_DEFAULT}), and postings are exactly what
     * these tests are about.
     */
    protected static Settings codecSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    protected MapperService codecMapperService() throws IOException {
        return createMapperService(
            codecSettings(),
            mapping(b -> b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).endObject())
        );
    }

    private List<IndexableField> fieldsFor(MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> doc)
        throws IOException {
        List<IndexableField> fields = new ArrayList<>();
        for (IndexableField field : mapperService.documentMapper().parse(source(doc)).rootDoc().getFields()) {
            if (field.name().equals(FIELD)) {
                fields.add(field);
            }
        }
        return fields;
    }

    private static List<IndexableField> postings(List<IndexableField> fields) {
        return fields.stream().filter(f -> f.fieldType().indexOptions() != IndexOptions.NONE).toList();
    }

    private static List<IndexableField> docValues(List<IndexableField> fields) {
        return fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).toList();
    }

    /**
     * Indexes a document holding a value and then {@code other} into one segment. Any disagreement over the field's index options is
     * raised here, by Lucene, when the second document is added.
     */
    private void indexAlongsideValue(MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> other) throws IOException {
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field(FIELD, sampleValue()))).rootDoc());
            iw.addDocument(mapperService.documentMapper().parse(source(other)).rootDoc());
        }, reader -> assertEquals(2, reader.numDocs()));
    }

    public void testBareNullIsDroppedEntirely() throws IOException {
        MapperService mapperService = codecMapperService();
        assertThat(
            "a bare null reaches neither the postings nor the doc values",
            fieldsFor(mapperService, b -> b.nullField(FIELD)),
            empty()
        );
    }

    public void testBareNullIndexesAlongsideValue() throws IOException {
        indexAlongsideValue(codecMapperService(), b -> b.nullField(FIELD));
    }

    /** The payload is the whole of what an all-null array leaves behind, and it states the field's index options itself. */
    public void testNullInArrayKeepsSlotAndStatesIndexOptions() throws IOException {
        MapperService mapperService = codecMapperService();
        List<IndexableField> fields = fieldsFor(mapperService, b -> b.startArray(FIELD).nullValue().endArray());
        assertEquals("the field is carried once", 1, fields.size());
        assertEquals("the null slot is kept in the doc values", 1, docValues(fields).size());
        assertEquals("and the same field states the index options", 1, postings(fields).size());
    }

    /**
     * A document that indexed a value alongside its nulls already has the field's index options from the value, so its payload
     * reports the plain doc-values type and adds nothing.
     */
    public void testArrayWithValueAndNullGetsNoEmptyPostings() throws IOException {
        MapperService mapperService = codecMapperService();
        List<IndexableField> fields = fieldsFor(
            mapperService,
            b -> b.startArray(FIELD).value(sampleValue()).nullValue().value(sampleValue()).endArray()
        );
        assertEquals("only the two values are indexed", 2, postings(fields).size());
    }

    /**
     * A mapper with a {@code null_value} puts a value in a null's place, so the array did index something after all and the payload
     * holds a real slot. Skipped for the types that have no {@code null_value} parameter.
     */
    public void testNullValueSubstitutionGetsNoEmptyPostings() throws IOException {
        assumeTrue(fieldTypeName() + " has no null_value parameter", supportsNullValue());
        MapperService mapperService = createMapperService(
            codecSettings(),
            mapping(b -> b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).field("null_value", "NA").endObject())
        );
        assertEquals(
            "one null becomes one substituted value and nothing else",
            1,
            postings(fieldsFor(mapperService, b -> b.startArray(FIELD).nullValue().endArray())).size()
        );
        assertEquals(
            "and two nulls become two",
            2,
            postings(fieldsFor(mapperService, b -> b.startArray(FIELD).nullValue().nullValue().endArray())).size()
        );
    }

    /** Whether the field type takes a {@code null_value}; the text types do not. */
    protected boolean supportsNullValue() {
        return false;
    }

    /**
     * An array of objects flattens every element's leaf onto one field, so a field can be written more than once in a document, and
     * the all-null array may come first. Whether the document indexed anything is therefore only settled once the whole document has
     * been read — not when an individual array ends.
     */
    public void testObjectArrayWritesFieldTwice() throws IOException {
        MapperService mapperService = createMapperService(
            codecSettings(),
            mapping(b -> b.startObject("outer." + FIELD).field("type", fieldTypeName()).field("index", true).endObject())
        );
        for (String source : List.of(
            "{\"outer\":[{\"" + FIELD + "\":[\"" + sampleValue() + "\"]},{\"" + FIELD + "\":[null]}]}",
            "{\"outer\":[{\"" + FIELD + "\":[null]},{\"" + FIELD + "\":[\"" + sampleValue() + "\"]}]}"
        )) {
            List<IndexableField> fields = new ArrayList<>();
            for (IndexableField field : mapperService.documentMapper()
                .parse(new SourceToParse("1", new BytesArray(source), XContentType.JSON))
                .rootDoc()
                .getFields()) {
                if (field.name().equals("outer." + FIELD)) {
                    fields.add(field);
                }
            }
            assertEquals(source + " indexes only the value, whichever order it arrives in", 1, postings(fields).size());
        }
    }

    /** A field written as two valueless arrays shares one payload, so it still carries the field once. */
    public void testObjectArrayWritesFieldTwiceWithNoValue() throws IOException {
        MapperService mapperService = createMapperService(
            codecSettings(),
            mapping(b -> b.startObject("outer." + FIELD).field("type", fieldTypeName()).field("index", true).endObject())
        );
        String source = "{\"outer\":[{\"" + FIELD + "\":[null]},{\"" + FIELD + "\":[null]}]}";
        List<IndexableField> fields = new ArrayList<>();
        for (IndexableField field : mapperService.documentMapper()
            .parse(new SourceToParse("1", new BytesArray(source), XContentType.JSON))
            .rootDoc()
            .getFields()) {
            if (field.name().equals("outer." + FIELD)) {
                fields.add(field);
            }
        }
        assertEquals("the field is carried once, not once per array", 1, postings(fields).size());
    }

    /**
     * A nested object yields a Lucene document per array element, so the payload an all-null array leaves behind belongs to the
     * element's document, not the root, and states its index options there.
     */
    public void testNestedDocumentsEachGetTheirOwn() throws IOException {
        MapperService mapperService = createMapperService(codecSettings(), mapping(b -> {
            b.startObject("n").field("type", "nested");
            b.startObject("properties");
            b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).endObject();
            b.endObject().endObject();
        }));
        String source = "{\"n\":[{\"" + FIELD + "\":[null]},{\"" + FIELD + "\":[\"" + sampleValue() + "\"]}]}";
        ParsedDocument parsed = mapperService.documentMapper().parse(new SourceToParse("1", new BytesArray(source), XContentType.JSON));

        for (LuceneDocument doc : parsed.docs()) {
            List<IndexableField> fields = new ArrayList<>();
            for (IndexableField field : doc.getFields()) {
                if (field.name().equals("n." + FIELD)) {
                    fields.add(field);
                }
            }
            if (fields.isEmpty()) {
                continue; // the root document, which carries none of this
            }
            assertEquals("every document carrying the field states its index options", 1, postings(fields).size());
        }
        // Lucene is the real check: a document left without them fails the whole batch.
        withLuceneIndex(mapperService, iw -> iw.addDocuments(parsed.docs()), reader -> assertEquals(3, reader.maxDoc()));
    }

    public void testAllNullArrayIndexesAlongsideValue() throws IOException {
        indexAlongsideValue(codecMapperService(), b -> b.startArray(FIELD).nullValue().endArray());
    }

    public void testMultipleNullArrayIndexesAlongsideValue() throws IOException {
        indexAlongsideValue(codecMapperService(), b -> b.startArray(FIELD).nullValue().nullValue().endArray());
    }

    public void testEmptyArrayIndexesAlongsideValue() throws IOException {
        indexAlongsideValue(codecMapperService(), b -> b.startArray(FIELD).endArray());
    }

    public void testArrayWithValueAndNullIndexesAlongsideValue() throws IOException {
        indexAlongsideValue(codecMapperService(), b -> b.startArray(FIELD).value(sampleValue()).nullValue().endArray());
    }

    public void testAbsentFieldIndexesAlongsideValue() throws IOException {
        indexAlongsideValue(codecMapperService(), b -> {});
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // The ESCF batch path. The mappers map a whole batch column-at-a-time when the shard takes that route, so the rules above have
    // to hold there too — and the disagreement they exist to prevent is only raised once Lucene sees the documents, which for this
    // path means IndexWriter#addBatch rather than #addDocument.
    // ---------------------------------------------------------------------------------------------------------------------------

    /**
     * Maps {@code sources} through the batch path and hands the resulting columns to {@code test}, having applied the engine values
     * the way {@code InternalEngine} does before indexing them.
     */
    private void withMappedColumns(MapperService mapperService, List<String> sources, CheckedConsumer<MappedColumns, IOException> test)
        throws IOException {
        final MappingLookup mappingLookup = mapperService.mappingLookup();
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        assertTrue(
            "the field must take the batch path, otherwise this test proves nothing",
            ((FieldMapper) mappingLookup.getMapper(FIELD)).supportsColumnarParse(indexSettings)
        );

        final int docCount = sources.size();
        final BytesReference[] sourceBytes = new BytesReference[docCount];
        final IndexRequest[] requests = new IndexRequest[docCount];
        for (int i = 0; i < docCount; i++) {
            sourceBytes[i] = new BytesArray(sources.get(i).getBytes(StandardCharsets.UTF_8));
            requests[i] = new IndexRequest("index").id("d" + i).source(sourceBytes[i], XContentType.JSON);
        }

        try (
            BatchMappingContext ctx = new BatchMappingContext(
                EngineTestCase.initFromRequests(requests),
                mappingLookup,
                indexSettings,
                new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY))
            )
        ) {
            final List<MetadataFieldMapper> metadata = Arrays.stream(mappingLookup.getMapping().getSortedMetadataMappers())
                .filter(m -> m.supportsColumnarParse(indexSettings))
                .toList();
            for (MetadataFieldMapper m : metadata) {
                m.preColumnarParse(ctx);
            }
            try (EscfBatch escfBatch = EscfEncoder.encode(Arrays.asList(sourceBytes), XContentType.JSON)) {
                final SourceSchema schema = escfBatch.schema();
                for (int c = 0; c < schema.leafCount(); c++) {
                    if (mappingLookup.getMapper(schema.getFullPath(c)) instanceof FieldMapper fm) {
                        fm.mapColumnBatch(ctx, escfBatch.column(c));
                    }
                }
                for (MetadataFieldMapper m : metadata) {
                    m.postColumnarParse(ctx);
                }
                final MappedColumns columns = ctx.columns();
                columns.fillPrimaryTerm(1L);
                for (int i = 0; i < docCount; i++) {
                    columns.setSeqNo(i, i);
                    columns.setVersion(i, 1L);
                }
                test.accept(columns);
            }
        }
    }

    /**
     * Indexes {@code columns} through {@code IndexWriter#addBatch}, the call {@code InternalEngine} makes for a mapped batch. Any
     * disagreement over the field's index options is raised here, by Lucene, as the batch is added.
     */
    private void addBatch(MapperService mapperService, MappedColumns columns, int expectedDocs) throws IOException {
        final IndexWriterConfig iwc = new IndexWriterConfig(
            IndexShard.buildIndexAnalyzer(mapperService, mapperService.getMapperMetrics().tokenCountingMetrics())
        ).setCodec(
            new PerFieldMapperCodec(
                Lucene104Codec.Mode.BEST_SPEED,
                ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                mapperService,
                BigArrays.NON_RECYCLING_INSTANCE,
                null
            )
        );
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, iwc)) {
            iw.addBatch(columns.toColumnBatch());
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                assertEquals(expectedDocs, reader.numDocs());
            }
        }
    }

    /** Maps a valued document alongside {@code other} through the batch path and indexes both. */
    private void addBatchAlongsideValue(String other) throws IOException {
        MapperService mapperService = codecMapperService();
        withMappedColumns(
            mapperService,
            List.of("{\"" + FIELD + "\":\"" + sampleValue() + "\"}", other),
            columns -> addBatch(mapperService, columns, 2)
        );
    }

    /** The columns the batch path produced for {@code FIELD}, by class, so their presence can be asserted. */
    private List<Column> fieldColumns(MappedColumns columns) {
        final List<Column> found = new ArrayList<>();
        final ColumnBatch batch = columns.toColumnBatch();
        for (Column column : batch.columns()) {
            if (column.name().equals(FIELD)) {
                found.add(column);
            }
        }
        return found;
    }

    public void testBatchBareNullIndexesAlongsideValue() throws IOException {
        addBatchAlongsideValue("{\"" + FIELD + "\":null}");
    }

    public void testBatchAllNullArrayIndexesAlongsideValue() throws IOException {
        addBatchAlongsideValue("{\"" + FIELD + "\":[null]}");
    }

    public void testBatchMultipleNullArrayIndexesAlongsideValue() throws IOException {
        addBatchAlongsideValue("{\"" + FIELD + "\":[null,null]}");
    }

    public void testBatchArrayWithValueAndNullIndexesAlongsideValue() throws IOException {
        addBatchAlongsideValue("{\"" + FIELD + "\":[\"" + sampleValue() + "\",null]}");
    }

    public void testBatchEmptyArrayIndexesAlongsideValue() throws IOException {
        addBatchAlongsideValue("{\"" + FIELD + "\":[]}");
    }

    public void testBatchAbsentFieldIndexesAlongsideValue() throws IOException {
        addBatchAlongsideValue("{}");
    }

    /** A bare null reaches no column at all, so the document does not carry the field. */
    public void testBatchBareNullProducesNoColumnEntry() throws IOException {
        MapperService mapperService = codecMapperService();
        withMappedColumns(mapperService, List.of("{\"" + FIELD + "\":null}"), columns -> {
            for (Column column : fieldColumns(columns)) {
                assertThat("a bare null produces no entry in any column for the field", docsIn(column), empty());
            }
        });
    }

    /**
     * The row cursor replays a batch's columns a document at a time, through the same {@code addDocument} that the row path uses, so
     * an all-null document has to be given the field's index options there just as it is when parsed from x-content.
     */
    public void testBatchRowCursorGivesAllNullArrayItsIndexOptions() throws IOException {
        MapperService mapperService = codecMapperService();
        withMappedColumns(
            mapperService,
            List.of("{\"" + FIELD + "\":\"" + sampleValue() + "\"}", "{\"" + FIELD + "\":[null]}"),
            columns -> {
                final MappedColumns.RowCursor cursor = columns.rowCursor();
                cursor.advance(); // the document holding a value
                cursor.advance();
                final List<IndexableField> allNullDoc = new ArrayList<>();
                for (IndexableField field : cursor.fields()) {
                    if (field.name().equals(FIELD)) {
                        allNullDoc.add(field);
                    }
                }
                assertEquals("the null slot is kept in the doc values", 1, docValues(allNullDoc).size());
                assertEquals("and the field is still given its index options", 1, postings(allNullDoc).size());
            }
        );
    }

    /**
     * A batch settles the field's index options on the column carrying its values, and Lucene rejects a second column claiming the
     * same for one field. The doc-values column varies its type per document only on the row path, never here.
     */
    public void testBatchClaimsInversionOnce() throws IOException {
        MapperService mapperService = codecMapperService();
        withMappedColumns(
            mapperService,
            List.of("{\"" + FIELD + "\":\"" + sampleValue() + "\"}", "{\"" + FIELD + "\":[null]}"),
            columns -> {
                long inverted = fieldColumns(columns).stream().filter(c -> c.fieldType().indexOptions() != IndexOptions.NONE).count();
                assertThat("at most one column may claim inversion for a field", inverted, lessThanOrEqualTo(1L));
            }
        );
    }

    /** The batch-local doc ids a column has an entry for. */
    private static List<Integer> docsIn(Column column) {
        final List<Integer> docs = new ArrayList<>();
        if (column instanceof BinaryColumn binaryColumn) {
            final ObjectTupleCursor<BytesRef> cursor = binaryColumn.tuples();
            for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                docs.add(doc);
            }
        }
        return docs;
    }

    /**
     * Stating the index options must not make the all-null document findable: the payload inverts into nothing, so the bytes it
     * carries never become a term.
     */
    public void testValuelessPayloadAddsNoTerm() throws IOException {
        MapperService mapperService = codecMapperService();
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field(FIELD, sampleValue()))).rootDoc());
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.startArray(FIELD).nullValue().endArray())).rootDoc());
        }, reader -> {
            var terms = reader.leaves().get(0).reader().terms(FIELD);
            if (terms == null) {
                return;
            }
            var it = terms.iterator();
            while (it.next() != null) {
                assertEquals("only the document holding a value has a term", 1, it.docFreq());
            }
        });
    }
}
