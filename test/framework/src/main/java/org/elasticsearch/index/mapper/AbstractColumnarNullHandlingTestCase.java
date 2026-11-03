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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.ElasticsearchStoredFieldsFormat;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    private static final String TSDB_DIMENSION = "dimension";
    private static final int TSDB_ROUTING_HASH = 17;
    private static final BytesRef TSDB_TSID = new BytesRef(new byte[] { 0x10, 0x20, 0x30, 0x40, 0x50 });

    protected abstract String fieldTypeName();

    /** A value of the field's type that indexes to at least one term, used as the document the all-null document has to agree with. */
    protected String sampleValue() {
        return "a value";
    }

    /**
     * A columnar index with the codec on and the field indexed. {@code index} is set explicitly because a strictly columnar index
     * defaults it to {@code false} for keyword (see {@code IndexSettings#INDEX_DISABLED_BY_DEFAULT}), and postings are exactly what
     * these tests are about.
     */
    protected static Settings codecSettings() {
        return columnarSettings();
    }

    /** A strictly columnar index. */
    protected static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
    }

    /**
     * The version to create a strictly columnar index at, so that the ColumNAR codec writes its doc values or, for an index created
     * before {@link IndexVersions#COLUMNAR_CODEC_NO_FEATURE_FLAG}, the in-order column does.
     */
    protected static IndexVersion columnarVersion(boolean withCodec) {
        return withCodec ? IndexVersion.current() : IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG);
    }

    protected MapperService codecMapperService() throws IOException {
        return createMapperService(
            codecSettings(),
            mapping(b -> b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).endObject())
        );
    }

    /**
     * Asserts that {@code doc} comes back out of synthetic source as {@code expected}, in a strictly columnar index both with the
     * ColumNAR codec and without it.
     *
     * <p>Which nulls survive is a property of the index mode, not of the format writing the doc values, so the two layouts have to
     * give the same answer. Asserting them together rather than one per run is what would catch them drifting apart.
     */
    protected void assertSourceUnderBothCodecs(String expected, CheckedConsumer<XContentBuilder, IOException> doc) throws IOException {
        for (boolean withCodec : new boolean[] { true, false }) {
            MapperService mapperService = createMapperService(
                columnarVersion(withCodec),
                columnarSettings(),
                mapping(b -> b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).endObject())
            );
            // Otherwise a version that quietly stopped selecting the codec would leave both halves running the same layout, and the
            // agreement this asserts would be worth nothing.
            assertEquals(
                "the index version has to select the codec for this to compare anything",
                withCodec,
                ColumnarDocValuesFormatSelector.useColumnarCodec(mapperService.getIndexSettings())
            );
            assertEquals("codec=" + withCodec, expected, syntheticSource(mapperService.documentMapper(), doc));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // What a null means, read back through synthetic source. The rule is the index mode's, so each of these holds whichever format
    // is writing; the sections below are about the ColumNAR payload in particular.
    // ---------------------------------------------------------------------------------------------------------------------------

    public void testBareNullIsAbsent() throws IOException {
        assertSourceUnderBothCodecs("{}", b -> b.nullField(FIELD));
    }

    public void testEmptyArrayIsAbsent() throws IOException {
        assertSourceUnderBothCodecs("{}", b -> b.startArray(FIELD).endArray());
    }

    public void testNullInArrayKeepsItsPlace() throws IOException {
        assertSourceUnderBothCodecs("""
            {"field":[null]}""", b -> b.startArray(FIELD).nullValue().endArray());
    }

    public void testAllNullArrayKeepsEverySlot() throws IOException {
        assertSourceUnderBothCodecs("""
            {"field":[null,null]}""", b -> b.startArray(FIELD).nullValue().nullValue().endArray());
    }

    public void testNullBetweenValuesKeepsItsPlace() throws IOException {
        assertSourceUnderBothCodecs("""
            {"field":["a",null,"b"]}""", b -> b.startArray(FIELD).value("a").nullValue().value("b").endArray());
    }

    /**
     * {@code exists} asks whether the document wrote at least one slot, which is the same question the source round-trips above
     * answer: a value or a null inside an array means the field is there, a bare null and an empty array mean it is not. The two
     * layouts reach it differently — the ColumNAR payload is written for such a document, the in-order column writes only its
     * companion count — so the query differs while the answer must not.
     */
    public void testExistsAsksForAtLeastOneSlot() throws IOException {
        final List<String> shapes = List.of(
            "{\"" + FIELD + "\":\"a\"}",
            "{\"" + FIELD + "\":[null]}",
            "{\"" + FIELD + "\":[]}",
            "{\"" + FIELD + "\":null}",
            "{}"
        );
        for (boolean withCodec : new boolean[] { true, false }) {
            MapperService mapperService = createMapperService(
                columnarVersion(withCodec),
                columnarSettings(),
                mapping(b -> b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).endObject())
            );
            final List<String> matched = new ArrayList<>();
            withLuceneIndex(mapperService, iw -> {
                for (int i = 0; i < shapes.size(); i++) {
                    iw.addDocument(
                        mapperService.documentMapper()
                            .parse(new SourceToParse(Integer.toString(i), new BytesArray(shapes.get(i)), XContentType.JSON))
                            .rootDoc()
                    );
                }
            }, reader -> {
                final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
                final int[] docs = Arrays.stream(new IndexSearcher(reader).search(exists, shapes.size()).scoreDocs)
                    .mapToInt(scoreDoc -> scoreDoc.doc)
                    .sorted()
                    .toArray();
                for (int doc : docs) {
                    matched.add(shapes.get(doc));
                }
            });
            assertEquals("codec=" + withCodec, List.of(shapes.get(0), shapes.get(1)), matched);
        }
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

    /**
     * A null inside an array of objects is a bare null, not an element of the field's own array: the position it would be kept in
     * belongs to the objects, and flattening {@code obj: [{f: null}, {f: "a"}]} into {@code f: [null, "a"]} is not an order this
     * promises to keep. So it is dropped, like any other bare null.
     *
     * <p>Strict columnar has no object mappers to put the parser in an array scope, so this is what the broader
     * {@code isPartOfArray()} resolves to there anyway; the test is here to keep the two from drifting apart.
     */
    public void testBareNullInObjectArrayIsDropped() throws IOException {
        MapperService mapperService = createMapperService(
            codecSettings(),
            mapping(b -> b.startObject("outer." + FIELD).field("type", fieldTypeName()).field("index", true).endObject())
        );
        String source = "{\"outer\":[{\"" + FIELD + "\":null},{\"" + FIELD + "\":\"" + sampleValue() + "\"}]}";
        List<IndexableField> fields = new ArrayList<>();
        for (IndexableField field : mapperService.documentMapper()
            .parse(new SourceToParse("1", new BytesArray(source), XContentType.JSON))
            .rootDoc()
            .getFields()) {
            if (field.name().equals("outer." + FIELD)) {
                fields.add(field);
            }
        }
        assertEquals("the value is indexed", 1, postings(fields).size());
        assertEquals("and carried once in the doc values, with no slot for the null", 1, docValues(fields).size());
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
        assertTrue("the field must take the batch path, otherwise this test proves nothing", takesBatchPath(mapperService));

        final int docCount = sources.size();
        final BytesReference[] sourceBytes = new BytesReference[docCount];
        final IndexRequest[] requests = new IndexRequest[docCount];
        final boolean tsdb = indexSettings.getMode().isTsdb();
        for (int i = 0; i < docCount; i++) {
            sourceBytes[i] = new BytesArray(sources.get(i).getBytes(StandardCharsets.UTF_8));
            requests[i] = new IndexRequest("index").id(tsdb ? null : "d" + i).source(sourceBytes[i], XContentType.JSON);
            if (tsdb) {
                requests[i].routing(TimeSeriesRoutingHashFieldMapper.encode(TSDB_ROUTING_HASH)).tsid(TSDB_TSID);
            }
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
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, batchIndexWriterConfig(mapperService))) {
            iw.addBatch(columns.toColumnBatch());
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                assertEquals(expectedDocs, reader.numDocs());
            }
        }
    }

    /**
     * Indexes a valued batch, a batch holding a single all-null array and a batch of random documents that hold no value, into one
     * writer, with the valued batch first or last, through whichever path the index maps the field on.
     */
    private void assertValuelessBatchesIndex(IndexMode indexMode, boolean withCodec, boolean valuedFirst) throws IOException {
        final MapperService mapperService = createMapperService(
            columnarVersion(withCodec),
            indexModeSettings(indexMode),
            indexModeMapping(indexMode)
        );
        final boolean batchPath = takesBatchPath(mapperService);
        if (indexMode.isStrictColumnar()) {
            assertTrue("a strictly columnar index must map the field on the batch path", batchPath);
        }
        final List<String> valued = valuedBatch(mapperService);
        final List<String> singleAllNullArray = List.of(allNullArrayDoc(mapperService));
        final List<String> valueless = valuelessBatch(mapperService);
        indexBatches(
            mapperService,
            batchPath,
            valuedFirst ? List.of(valued, singleAllNullArray, valueless) : List.of(singleAllNullArray, valueless, valued),
            valued.size()
        );
    }

    /**
     * Indexes {@code batches} one after another into the same writer, the way a shard receives one bulk request after another, and
     * checks that only the {@code valuedDocs} documents holding a value got postings for the field.
     */
    private void indexBatches(MapperService mapperService, boolean batchPath, List<List<String>> batches, int valuedDocs)
        throws IOException {
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, batchIndexWriterConfig(mapperService))) {
            int expectedDocs = 0;
            for (List<String> batch : batches) {
                if (batchPath) {
                    withMappedColumns(mapperService, batch, columns -> iw.addBatch(columns.toColumnBatch()));
                } else {
                    iw.addDocuments(parseRows(mapperService, batch));
                }
                expectedDocs += batch.size();
            }
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                final String index = "["
                    + mapperService.getIndexSettings().getMode()
                    + "] "
                    + mapperService.getIndexSettings().getIndexVersionCreated();
                assertEquals(index, expectedDocs, reader.numDocs());
                assertEquals(index + " only the documents holding a value have postings", valuedDocs, docsWithPostings(reader));
            }
        }
    }

    private static int docsWithPostings(DirectoryReader reader) throws IOException {
        int docs = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            final Terms terms = leaf.reader().terms(FIELD);
            if (terms == null) {
                continue;
            }
            final FixedBitSet withPostings = new FixedBitSet(leaf.reader().maxDoc());
            final TermsEnum termsEnum = terms.iterator();
            PostingsEnum postings = null;
            while (termsEnum.next() != null) {
                postings = termsEnum.postings(postings, PostingsEnum.NONE);
                for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                    withPostings.set(doc);
                }
            }
            docs += withPostings.cardinality();
        }
        return docs;
    }

    private static boolean takesBatchPath(MapperService mapperService) {
        return ((FieldMapper) mapperService.mappingLookup().getMapper(FIELD)).supportsColumnarParse(mapperService.getIndexSettings());
    }

    private static List<LuceneDocument> parseRows(MapperService mapperService, List<String> sources) {
        final boolean tsdb = mapperService.getIndexSettings().getMode().isTsdb();
        final List<LuceneDocument> docs = new ArrayList<>();
        for (String source : sources) {
            final SourceToParse toParse = new SourceToParse(
                tsdb ? null : "row",
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON,
                tsdb ? TimeSeriesRoutingHashFieldMapper.encode(TSDB_ROUTING_HASH) : null
            );
            docs.addAll(mapperService.documentMapper().parse(toParse).docs());
        }
        return docs;
    }

    private static Settings indexModeSettings(IndexMode indexMode) {
        final Settings.Builder settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName());
        if (indexMode.isTsdb()) {
            settings.putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), TSDB_DIMENSION)
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false);
        }
        return settings.build();
    }

    private XContentBuilder indexModeMapping(IndexMode indexMode) throws IOException {
        return mapping(b -> {
            if (indexMode.isTsdb()) {
                b.startObject(TSDB_DIMENSION).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
            b.startObject(FIELD).field("type", fieldTypeName()).field("index", true).endObject();
        });
    }

    private static String indexModeDoc(MapperService mapperService, @Nullable String field) {
        final List<String> entries = new ArrayList<>();
        if (mapperService.mappingLookup().isDataStreamTimestampFieldEnabled()) {
            entries.add("\"@timestamp\":1735689600000");
        }
        if (mapperService.getIndexSettings().getMode().isTsdb()) {
            entries.add("\"" + TSDB_DIMENSION + "\":\"a\"");
        }
        if (field != null) {
            entries.add(field);
        }
        return "{" + String.join(",", entries) + "}";
    }

    private static IndexWriterConfig batchIndexWriterConfig(MapperService mapperService) {
        return new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService, mapperService.getMapperMetrics().tokenCountingMetrics()))
            .setCodec(
                new PerFieldMapperCodec(
                    Lucene104Codec.Mode.BEST_SPEED,
                    ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                    ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                    mapperService,
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null
                )
            );
    }

    /** A batch in which no document holds a value, with an all-null array among them so that the field still writes a payload. */
    private static List<String> valuelessBatch(MapperService mapperService) {
        final List<String> batch = new ArrayList<>();
        batch.add(allNullArrayDoc(mapperService));
        final int extra = randomIntBetween(0, 4);
        for (int i = 0; i < extra; i++) {
            batch.add(
                randomFrom(
                    allNullArrayDoc(mapperService),
                    indexModeDoc(mapperService, "\"" + FIELD + "\":[null,null]"),
                    indexModeDoc(mapperService, "\"" + FIELD + "\":[]"),
                    indexModeDoc(mapperService, null)
                )
            );
        }
        Collections.shuffle(batch, random());
        return batch;
    }

    private static String allNullArrayDoc(MapperService mapperService) {
        return indexModeDoc(mapperService, "\"" + FIELD + "\":[null]");
    }

    private List<String> valuedBatch(MapperService mapperService) {
        return List.of(indexModeDoc(mapperService, "\"" + FIELD + "\":\"" + sampleValue() + "\""));
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

    public void testStandardValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.STANDARD, true, true);
    }

    public void testStandardValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.STANDARD, true, false);
    }

    public void testTimeSeriesValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.TIME_SERIES, true, true);
    }

    public void testTimeSeriesValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.TIME_SERIES, true, false);
    }

    public void testLogsdbValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOGSDB, true, true);
    }

    public void testLogsdbValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOGSDB, true, false);
    }

    public void testLookupValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOOKUP, true, true);
    }

    public void testLookupValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOOKUP, true, false);
    }

    public void testColumnarValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.COLUMNAR, true, true);
    }

    public void testColumnarValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.COLUMNAR, true, false);
    }

    public void testColumnarWithoutCodecValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.COLUMNAR, false, true);
    }

    public void testColumnarWithoutCodecValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.COLUMNAR, false, false);
    }

    public void testLogsdbColumnarValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOGSDB_COLUMNAR, true, true);
    }

    public void testLogsdbColumnarValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOGSDB_COLUMNAR, true, false);
    }

    public void testLogsdbColumnarWithoutCodecValuelessBatchAfterValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOGSDB_COLUMNAR, false, true);
    }

    public void testLogsdbColumnarWithoutCodecValuelessBatchBeforeValuedBatch() throws IOException {
        assertValuelessBatchesIndex(IndexMode.LOGSDB_COLUMNAR, false, false);
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
