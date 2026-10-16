/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOConsumer;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.Build;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibrationProfile;
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase.randomNormalizedVector;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.getIndexOptions;
import static org.elasticsearch.index.mapper.vectors.DenseVectorTestSettingsBuilder.EXPERIMENTAL_FEATURES_DISABLED;
import static org.elasticsearch.index.mapper.vectors.DenseVectorTestSettingsBuilder.EXPERIMENTAL_FEATURES_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFieldMapperTests extends SyntheticVectorsMapperTestCase {

    private static final IndexVersion INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION = IndexVersions.V_8_10_0;
    private final ElementType elementType;
    private final boolean indexed;
    private final boolean indexOptionsSet;
    private final int dims;

    public DenseVectorFieldMapperTests() {
        this.elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT, ElementType.BFLOAT16, ElementType.BIT);
        this.indexed = usually();
        this.indexOptionsSet = this.indexed && randomBoolean();
        int baseDims = ElementType.BIT == elementType ? 4 * Byte.SIZE : 4;
        int randomMultiplier = switch (elementType) {
            case FLOAT, BFLOAT16 -> randomIntBetween(1, 64);
            case BYTE, BIT -> 1;
        };
        this.dims = baseDims * randomMultiplier;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        indexMapping(b, IndexVersion.current());
    }

    @Override
    protected void minimalMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        indexMapping(b, indexVersion);
    }

    @Override
    public void testEmbeddingsFieldAndFormat() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = mapperService.fieldType("field");
        assertEquals(new FieldAndFormat("field", null), fieldType.embeddingsFieldAndFormat(null));
        assertEquals(new FieldAndFormat("field", null), fieldType.embeddingsFieldAndFormat(VectorType.DENSE_VECTOR));
        assertUnsupportedEmbeddings(fieldType, VectorType.SPARSE_VECTOR);
        assertParseMinimalWarnings();
    }

    @Override
    public void testNotIndexed() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::notIndexedMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", getSampleValueForDocument())));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).fieldType().vectorDimension(), equalTo(0));
        assertThat(fields.get(0).fieldType().docValuesType(), equalTo(DocValuesType.BINARY));
    }

    @Override
    public void testDisableDefaultIndex() throws IOException {
        var settings = new DenseVectorTestSettingsBuilder().indexDisabledByDefault(true).build();
        var mappingBuilder = new DenseVectorMappingBuilder().dims(dims);
        if (elementType != ElementType.FLOAT) {
            mappingBuilder.elementType(elementType);
        }

        var mapperService = createMapperService(settings, fieldMapping(mappingBuilder::build));
        var documentMapper = mapperService.documentMapper();

        ParsedDocument doc = documentMapper.parse(source(b -> b.field("field", this.getSampleValueForDocument())));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).fieldType().vectorDimension(), equalTo(0));
        assertThat(fields.get(0).fieldType().docValuesType(), equalTo(DocValuesType.BINARY));
    }

    /**
     * A non-indexed vector is held in binary doc values, so it is left out of the stored {@code _source} rather than written
     * a second time. A document indexed before the exclusion applied still carries it there; reading that one patches the
     * same value over it rather than adding a second copy.
     */
    public void testVectorIsNotStoredInSourceWhenNotIndexed() throws IOException {
        String mapping = Strings.toString(fieldMapping(this::notIndexedMapping));
        Object sample = getSampleValueForDocument(false);

        var settings = new DenseVectorTestSettingsBuilder().excludeSourceVectors(true).build();
        MapperService mapperService = createMapperService(settings, mapping);
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", sample)));
        assertThat(storedSource(doc).utf8ToString(), equalTo("{}"));

        var legacySettings = new DenseVectorTestSettingsBuilder().excludeSourceVectors(false).build();
        MapperService legacy = createMapperService(legacySettings, mapping);
        ParsedDocument legacyDoc = legacy.documentMapper().parse(source(b -> b.field("field", sample)));
        BytesReference legacySource = storedSource(legacyDoc);
        assertThat(legacySource.utf8ToString(), not("{}"));

        withLuceneIndex(mapperService, iw -> iw.addDocument(legacyDoc.rootDoc()), reader -> {
            var provider = SourceProvider.fromLookup(
                mapperService.mappingLookup(),
                null,
                mapperService.getMapperMetrics().sourceFieldMetrics(),
                null
            );
            Source loaded = provider.getSource(reader.leaves().get(0), 0);
            assertToXContentEquivalent(legacySource, loaded.internalSourceRef(), XContentType.JSON);
        });
    }

    private static BytesReference storedSource(ParsedDocument doc) {
        return new BytesArray(doc.rootDoc().getField(SourceFieldMapper.NAME).binaryValue());
    }

    @Override
    protected List<CheckedConsumer<XContentBuilder, IOException>> vectorMappings() {
        // A non-indexed vector is held in binary doc values rather than the vector index, so it is patched back into
        // _source by a different loader. Cover both, regardless of what `indexed` was randomized to.
        return List.of(this::minimalMapping, this::notIndexedMapping);
    }

    private void notIndexedMapping(XContentBuilder b) throws IOException {
        var mapping = new DenseVectorMappingBuilder().dims(dims).index(false);
        if (elementType != ElementType.FLOAT) {
            mapping.elementType(elementType);
        }
        mapping.build(b);
    }

    private void indexMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        // Serialize if it's new index version, or it was not the default for previous indices
        var mapping = new DenseVectorMappingBuilder().dims(dims);
        if (indexVersion.onOrAfter(DenseVectorFieldMapper.INDEXED_BY_DEFAULT_INDEX_VERSION) || indexed) {
            mapping.index(indexed);
        }

        if ((indexVersion.onOrAfter(DenseVectorFieldMapper.DEFAULT_TO_INT8)
            || indexVersion.onOrAfter(DenseVectorFieldMapper.DEFAULT_TO_BBQ))
            && indexed
            && DenseVectorFieldMapperTestUtils.elementTypesWithDefaultIndexOptions(indexVersion).contains(elementType)
            && indexOptionsSet == false) {
            if (indexVersion.onOrAfter(DenseVectorFieldMapper.DEFAULT_TO_BBQ)
                && dims >= DenseVectorFieldMapper.BBQ_DIMS_DEFAULT_THRESHOLD) {
                mapping.indexOptions(
                    Map.of("type", "bbq_hnsw", "m", 16, "ef_construction", 100, "rescore_vector", Map.of("oversample", DEFAULT_OVERSAMPLE))
                );
            } else {
                mapping.indexOptions(Map.of("type", "int8_hnsw", "m", 16, "ef_construction", 100));
            }
        }

        if (indexed) {
            mapping.similarity(elementType == ElementType.BIT ? VectorSimilarity.L2_NORM : VectorSimilarity.DOT_PRODUCT);
            if (indexOptionsSet) {
                mapping.indexOptions(Map.of("type", "hnsw", "m", 5, "ef_construction", 50));
            }
        }

        if (elementType != ElementType.FLOAT) {
            mapping.elementType(elementType);
        }

        mapping.build(b);
    }

    @Override
    protected Object getSampleValueForDocument(boolean binaryFormat) {
        if (binaryFormat) {
            byte[] toEncode = switch (elementType) {
                case FLOAT -> {
                    float[] array = randomNormalizedVector(this.dims);
                    final ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES * array.length);
                    buffer.asFloatBuffer().put(array);
                    yield buffer.array();
                }
                case BFLOAT16 -> {
                    float[] array = randomNormalizedVector(this.dims);
                    byte[] buffer = new byte[BFloat16.BYTES * array.length];
                    BFloat16.floatToBFloat16(array, 0, buffer, 0, dims, ByteOrder.BIG_ENDIAN);
                    yield buffer;
                }
                case BYTE -> randomByteArrayOfLength(dims);
                case BIT -> randomByteArrayOfLength(this.dims / Byte.SIZE);
            };
            return Base64.getEncoder().encodeToString(toEncode);
        } else {
            return switch (elementType) {
                case FLOAT -> convertToList(randomNormalizedVector(this.dims));
                case BFLOAT16 -> convertToBFloat16List(randomNormalizedVector(this.dims));
                case BYTE -> convertToList(randomByteArrayOfLength(dims));
                case BIT -> convertToList(randomByteArrayOfLength(this.dims / Byte.SIZE));
            };
        }
    }

    @Override
    protected Object getSampleValueForDocument() {
        return getSampleValueForDocument(randomBoolean());
    }

    public static List<Float> convertToList(float[] vector) {
        List<Float> list = new ArrayList<>(vector.length);
        for (float v : vector) {
            list.add(v);
        }
        return list;
    }

    public static List<Float> convertToBFloat16List(float[] vector) {
        List<Float> list = new ArrayList<>(vector.length);
        for (float v : vector) {
            list.add(BFloat16.truncateToBFloat16(v));
        }
        return list;
    }

    public static List<Byte> convertToList(byte[] vector) {
        List<Byte> list = new ArrayList<>(vector.length);
        for (byte v : vector) {
            list.add(v);
        }
        return list;
    }

    private static void registerConflict(
        ParameterChecker checker,
        String param,
        IOConsumer<XContentBuilder> base,
        String changeField,
        Object original,
        Object change
    ) throws IOException {
        registerConflict(checker, param, base, b -> b.field(changeField, original), b -> b.field(changeField, change));
    }

    private static void registerConflict(
        ParameterChecker checker,
        String param,
        IOConsumer<XContentBuilder> base,
        IOConsumer<XContentBuilder> original,
        IOConsumer<XContentBuilder> change
    ) throws IOException {
        checker.registerConflictCheck(param, fieldMapping(b -> {
            base.accept(b);
            original.accept(b);
        }), fieldMapping(b -> {
            base.accept(b);
            change.accept(b);
        }));
    }

    private static void registerIndexOptionsUpdate(
        ParameterChecker checker,
        IOConsumer<XContentBuilder> base,
        String changeOptionField,
        Object originalOption,
        Object changeOption,
        Matcher<FieldMapper> check
    ) throws IOException {
        registerIndexOptionsUpdate(
            checker,
            base,
            b -> b.field(changeOptionField, originalOption),
            b -> b.field(changeOptionField, changeOption),
            check
        );
    }

    private static void registerIndexOptionsUpdate(
        ParameterChecker checker,
        IOConsumer<XContentBuilder> base,
        IOConsumer<XContentBuilder> originalOptions,
        IOConsumer<XContentBuilder> changeOptions,
        Matcher<FieldMapper> check
    ) throws IOException {
        checker.registerUpdateCheck("index_options", b -> {
            base.accept(b);
            b.startObject("index_options");
            originalOptions.accept(b);
            b.endObject();
        }, b -> {
            base.accept(b);
            b.startObject("index_options");
            changeOptions.accept(b);
            b.endObject();
        }, m -> assertThat(m, check));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        var indexedMapping = new DenseVectorMappingBuilder().dims(dims).index(true);
        var bbqMapping = indexedMapping.clone().dims(dims * 16);

        registerConflict(checker, "dims", b -> new DenseVectorMappingBuilder().build(b), "dims", dims, dims + 8);
        registerConflict(checker, "similarity", indexedMapping::build, "similarity", "dot_product", "l2_norm");
        registerConflict(
            checker,
            "index",
            b -> new DenseVectorMappingBuilder().dims(dims).build(b),
            b -> b.field("index", true).field("similarity", "dot_product"),
            b -> b.field("index", false)
        );
        registerConflict(
            checker,
            "element_type",
            b -> indexedMapping.clone().similarity(VectorSimilarity.DOT_PRODUCT).build(b),
            "element_type",
            "byte",
            "float"
        );
        registerConflict(
            checker,
            "element_type",
            b -> new DenseVectorMappingBuilder().index(true).similarity(VectorSimilarity.L2_NORM).build(b),
            b -> b.field("dims", dims).field("element_type", "float"),
            b -> b.field("dims", dims * 8).field("element_type", "bit")
        );
        registerConflict(
            checker,
            "element_type",
            b -> new DenseVectorMappingBuilder().index(true).similarity(VectorSimilarity.L2_NORM).build(b),
            b -> b.field("dims", dims).field("element_type", "float"),
            b -> b.field("dims", dims * 8).field("element_type", "bit")
        );

        // update for flat
        for (String newType : List.of("int8_flat", "int4_flat", "hnsw", "int8_hnsw", "int4_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                indexedMapping::build,
                "type",
                "flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }
        for (String newType : List.of("bbq_flat", "bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }

        // update for int8_flat
        registerConflict(
            checker,
            "index_options",
            indexedMapping::build,
            b -> b.field("index_options", Map.of("type", "int8_flat")),
            b -> b.field("index_options", Map.of("type", "flat"))
        );
        for (String newType : List.of("int4_flat", "hnsw", "int8_hnsw", "int4_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                indexedMapping::build,
                "type",
                "int8_flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }
        for (String newType : List.of("bbq_flat", "bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "int8_flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }

        // update for hnsw
        for (String newType : List.of("flat", "int8_flat", "int4_flat")) {
            registerConflict(
                checker,
                "index_options",
                indexedMapping::build,
                b -> b.field("index_options", Map.of("type", "hnsw")),
                b -> b.field("index_options", Map.of("type", newType))
            );
        }
        for (String newType : List.of("int8_hnsw", "int4_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                indexedMapping::build,
                "type",
                "hnsw",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }
        registerIndexOptionsUpdate(
            checker,
            indexedMapping::build,
            b -> b.field("type", "hnsw"),
            b -> b.field("type", "hnsw").field("m", 100),
            hasToString(containsString("\"m\":100"))
        );
        registerConflict(
            checker,
            "index_options",
            indexedMapping::build,
            b -> b.field("index_options", Map.of("type", "hnsw", "m", 32)),
            b -> b.field("index_options", Map.of("type", "hnsw", "m", 16))
        );
        registerConflict(
            checker,
            "index_options",
            bbqMapping::build,
            b -> b.field("index_options", Map.of("type", "hnsw")),
            b -> b.field("index_options", Map.of("type", "bbq_flat"))
        );
        for (String newType : List.of("bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "hnsw",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }

        // update for int8_hnsw
        registerIndexOptionsUpdate(
            checker,
            indexedMapping::build,
            b -> b.field("type", "int8_hnsw"),
            b -> b.field("type", "int8_hnsw").field("m", 256),
            hasToString(containsString("\"m\":256"))
        );
        registerIndexOptionsUpdate(
            checker,
            indexedMapping::build,
            b -> b.field("type", "int8_hnsw"),
            b -> b.field("type", "int4_hnsw").field("m", 256),
            hasToString(containsString("\"type\":\"int4_hnsw\""))
        );
        registerConflict(
            checker,
            "index_options",
            indexedMapping::build,
            b -> b.field("index_options", Map.of("type", "int8_hnsw", "m", 32)),
            b -> b.field("index_options", Map.of("type", "int8_hnsw", "m", 16))
        );
        for (String newType : List.of("flat", "int8_flat", "int4_flat")) {
            registerConflict(
                checker,
                "index_options",
                indexedMapping::build,
                b -> b.field("index_options", Map.of("type", "int8_hnsw")),
                b -> b.field("index_options", Map.of("type", newType))
            );
        }
        registerConflict(
            checker,
            "index_options",
            bbqMapping::build,
            b -> b.field("index_options", Map.of("type", "int8_hnsw")),
            b -> b.field("index_options", Map.of("type", "bbq_flat"))
        );
        for (String newType : List.of("bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "int8_hnsw",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }

        // update for int4_flat
        for (String newType : List.of("hnsw", "int8_hnsw", "int4_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                indexedMapping::build,
                "type",
                "int4_flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }
        for (String newType : List.of("flat", "int8_flat")) {
            registerConflict(
                checker,
                "index_options",
                indexedMapping::build,
                b -> b.field("index_options", Map.of("type", "int4_flat")),
                b -> b.field("index_options", Map.of("type", newType))
            );
        }
        for (String newType : List.of("bbq_flat", "bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "int4_flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }

        // update for int4_hnsw
        registerIndexOptionsUpdate(
            checker,
            indexedMapping::build,
            b -> b.field("type", "int4_hnsw"),
            b -> b.field("type", "int4_hnsw").field("m", 256),
            hasToString(containsString("\"m\":256"))
        );
        registerIndexOptionsUpdate(
            checker,
            indexedMapping::build,
            b -> b.field("type", "int4_hnsw").field("m", 4),
            b -> b.field("type", "int4_hnsw").field("m", 100),
            hasToString(containsString("\"m\":100"))
        );
        registerConflict(
            checker,
            "index_options",
            indexedMapping::build,
            b -> b.field("index_options", Map.of("type", "int4_hnsw", "m", 32)),
            b -> b.field("index_options", Map.of("type", "int4_hnsw", "m", 16))
        );
        registerConflict(
            checker,
            "index_options",
            indexedMapping::build,
            b -> b.field("index_options", Map.of("type", "int4_hnsw", "m", 32)),
            b -> b.field("index_options", Map.of("type", "int8_hnsw", "m", 16))
        );
        registerConflict(
            checker,
            "index_options",
            indexedMapping::build,
            b -> b.field("index_options", Map.of("type", "int4_hnsw", "m", 32)),
            b -> b.field("index_options", Map.of("type", "hnsw", "m", 16))
        );
        for (String newType : List.of("flat", "int8_flat", "int4_flat")) {
            registerConflict(
                checker,
                "index_options",
                indexedMapping::build,
                b -> b.field("index_options", Map.of("type", "int4_hnsw")),
                b -> b.field("index_options", Map.of("type", newType))
            );
        }
        registerConflict(
            checker,
            "index_options",
            bbqMapping::build,
            b -> b.field("index_options", Map.of("type", "int4_hnsw")),
            b -> b.field("index_options", Map.of("type", "bbq_flat"))
        );
        for (String newType : List.of("bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "int4_hnsw",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }

        // update for bbq_flat
        for (String newType : List.of("bbq_hnsw")) {
            registerIndexOptionsUpdate(
                checker,
                bbqMapping::build,
                "type",
                "bbq_flat",
                newType,
                hasToString(containsString("\"type\":\"" + newType + "\""))
            );
        }
        for (String newType : List.of("flat", "int8_flat", "int4_flat", "hnsw", "int8_hnsw", "int4_hnsw")) {
            registerConflict(
                checker,
                "index_options",
                bbqMapping::build,
                b -> b.field("index_options", Map.of("type", "bbq_flat")),
                b -> b.field("index_options", Map.of("type", newType))
            );
        }

        // update for bbq_hnsw
        for (String newType : List.of("flat", "int8_flat", "int4_flat", "hnsw", "int8_hnsw", "int4_hnsw")) {
            registerConflict(
                checker,
                "index_options",
                bbqMapping::build,
                b -> b.field("index_options", Map.of("type", "bbq_hnsw")),
                b -> b.field("index_options", Map.of("type", newType))
            );
        }

        // update for bbq_disk
        registerIndexOptionsUpdate(
            checker,
            bbqMapping::build,
            b -> b.field("type", "bbq_disk").field("cluster_size", 1000),
            b -> b.field("type", "bbq_disk").field("cluster_size", 500),
            hasToString(containsString("\"cluster_size\":500"))
        );
        registerConflict(
            checker,
            "index_options",
            bbqMapping::build,
            b -> b.field("index_options", Map.of("type", "bbq_disk", "precondition", true)),
            b -> b.field("index_options", Map.of("type", "bbq_disk", "precondition", false))
        );
        registerIndexOptionsUpdate(
            checker,
            bbqMapping::build,
            b -> b.field("type", "bbq_disk").field("bits", 4),
            b -> b.field("type", "bbq_disk").field("bits", 2),
            hasToString(containsString("\"bits\":2"))
        );
        registerIndexOptionsUpdate(checker, bbqMapping::build, b -> {
            b.field("type", "bbq_disk").field("bits", 4);
            b.startObject("rescore_vector");
            b.field("oversample", 3f);
            b.endObject();
        }, b -> {
            b.field("type", "bbq_disk").field("bits", 4);
            b.startObject("rescore_vector");
            b.field("oversample", 4f);
            b.endObject();
        }, hasToString(containsString("\"oversample\":4.0")));
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(DenseVectorFieldType.class));
        if (indexed) {
            assertTrue(fieldType.indexType().hasVectors());
        } else {
            assertTrue(fieldType.indexType().hasOnlyDocValues());
        }
        assertEquals(fieldType.isSearchable(), indexed);
    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(FieldExistsQuery.class));
        FieldExistsQuery existsQuery = (FieldExistsQuery) query;
        assertEquals("field", existsQuery.getField());
        assertNoFieldNamesField(fields);
    }

    // We override this because dense vectors are the only field type that are not aggregatable but
    // that do provide fielddata. TODO: resolve this inconsistency!
    @Override
    public void testAggregatableConsistency() {}

    public void testIVFParsing() throws IOException {
        var base = new DenseVectorMappingBuilder().dims(128).index(true).similarity(VectorSimilarity.DOT_PRODUCT);
        {
            MapperService mapperService = createMapperService(
                EXPERIMENTAL_FEATURES_ENABLED,
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "bbq_disk", "bits", 4)).build(b))
            );

            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQIVFIndexOptions.class
            );
            assertEquals(4, indexOptions.bits, 0.0F);
            assertNull(indexOptions.rescoreVector);
            assertEquals(ES940DiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER, indexOptions.clusterSize);
            assertEquals(-1, indexOptions.getFlatIndexThreshold());
            assertEquals(0.0, indexOptions.defaultVisitPercentage, 0.0);
        }
        {
            MapperService mapperService = createMapperService(
                EXPERIMENTAL_FEATURES_ENABLED,
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "bbq_disk", "bits", 7)).build(b))
            );

            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQIVFIndexOptions.class
            );
            assertEquals(7, indexOptions.bits, 0.0F);
            assertNull(indexOptions.rescoreVector);
        }
        {
            MapperService mapperService = createMapperService(
                EXPERIMENTAL_FEATURES_DISABLED,
                fieldMapping(
                    b -> base.clone()
                        .indexOptions(
                            Map.of(
                                "type",
                                "bbq_disk",
                                "cluster_size",
                                1000,
                                "flat_index_threshold",
                                1500,
                                "default_visit_percentage",
                                5.0,
                                DenseVectorFieldMapper.RescoreVector.NAME,
                                Map.of("oversample", 2.0f)
                            )
                        )
                        .build(b)
                )
            );

            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQIVFIndexOptions.class
            );
            assertEquals(2F, indexOptions.rescoreVector.oversample(), 0.0F);
            assertEquals(1000, indexOptions.clusterSize);
            assertEquals(1500, indexOptions.getFlatIndexThreshold());
            assertEquals(5.0, indexOptions.defaultVisitPercentage, 0.0);
            assertEquals(1, indexOptions.bits, 0.0);
        }
        {
            MapperService mapperService = createMapperService(
                EXPERIMENTAL_FEATURES_DISABLED,
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "bbq_disk", "bits", 4)).build(b))
            );

            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQIVFIndexOptions.class
            );
            assertEquals(4, indexOptions.bits, 0.0F);
        }
        {
            MapperService mapperService = createMapperService(
                EXPERIMENTAL_FEATURES_DISABLED,
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "bbq_disk", "precondition", true)).build(b))
            );

            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQIVFIndexOptions.class
            );
            assertTrue(indexOptions.doPrecondition());
        }
    }

    public void testAutoCalibrateParsing() throws IOException {
        CheckedBiConsumer<Object, IvfAutoCalibrationProfile, IOException> assertParsing = (autoCalibrate, expectedProfile) -> {
            String message = "auto_calibrate [" + autoCalibrate + "]";
            MapperService mapperService = createMapperService(EXPERIMENTAL_FEATURES_ENABLED, autoCalibrateMapping(autoCalibrate));
            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQIVFIndexOptions.class
            );
            assertEquals(message, expectedProfile != IvfAutoCalibrationProfile.DISABLED, indexOptions.autoCalibrate());
            assertEquals(message, expectedProfile, indexOptions.autoCalibrationProfile());

            String mappingSource = mapperService.documentMapper().mappingSource().toString();
            if (autoCalibrate == null) {
                assertThat(message, mappingSource, not(containsString("auto_calibrate")));
            } else {
                String expectedSource = autoCalibrate instanceof String
                    ? "\"auto_calibrate\":\"" + autoCalibrate + "\""
                    : "\"auto_calibrate\":" + autoCalibrate;
                assertThat(message, mappingSource, containsString(expectedSource));
            }
        };

        assertParsing.accept(null, IvfAutoCalibrationProfile.DISABLED);
        assertParsing.accept(true, IvfAutoCalibrationProfile.ISO_SIZING);
        assertParsing.accept(false, IvfAutoCalibrationProfile.DISABLED);
        for (IvfAutoCalibrationProfile profile : IvfAutoCalibrationProfile.values()) {
            assertParsing.accept(profile.toString(), profile);
        }
    }

    public void testBBQDiskAutoCalibrateIndexOptionMappingInteractions() throws IOException {
        final var baseMapping = new DenseVectorMappingBuilder().dims(128).index(true);
        final Map<String, Object> baseIndexOptionsMap = Map.of(
            "type",
            "bbq_disk",
            "bits",
            4,
            "precondition",
            false,
            "auto_calibrate",
            true,
            "rescore_vector",
            Map.of("oversample", 3f)
        );

        MapperService mapperService = createMapperService(
            EXPERIMENTAL_FEATURES_ENABLED,
            fieldMapping(b -> baseMapping.clone().indexOptions(baseIndexOptionsMap).build(b))
        );

        final Map<String, Object> twoBitIndexOptionsMap = Maps.copyMapWithAddedOrReplacedEntry(baseIndexOptionsMap, "bits", 2);
        merge(mapperService, fieldMapping(b -> baseMapping.clone().indexOptions(twoBitIndexOptionsMap).build(b)));
        DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = getIndexOptions(
            mapperService,
            "field",
            DenseVectorFieldMapper.BBQIVFIndexOptions.class
        );
        assertEquals(2, indexOptions.getBits());
        assertTrue(indexOptions.autoCalibrate());

        final Map<String, Object> oversampleFourIndexOptionsMap = Maps.copyMapWithAddedOrReplacedEntry(
            twoBitIndexOptionsMap,
            "rescore_vector",
            Map.of("oversample", 4f)
        );
        merge(mapperService, fieldMapping(b -> baseMapping.clone().indexOptions(oversampleFourIndexOptionsMap).build(b)));
        indexOptions = getIndexOptions(mapperService, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class);
        assertEquals(4f, indexOptions.rescoreVector.oversample(), 0f);

        final Map<String, Object> autocalibrateDisabledIndexOptionsMap = Maps.copyMapWithAddedOrReplacedEntry(
            oversampleFourIndexOptionsMap,
            "auto_calibrate",
            false
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> baseMapping.clone().indexOptions(autocalibrateDisabledIndexOptionsMap).build(b)))
        );

        final Map<String, Object> preconditionTrueIndexOptionsMap = Maps.copyMapWithAddedOrReplacedEntry(
            oversampleFourIndexOptionsMap,
            "precondition",
            true
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> baseMapping.clone().indexOptions(preconditionTrueIndexOptionsMap).build(b)))
        );
    }

    public void testAutoCalibrateDefaultEnabledProfile() throws IOException {
        IndexVersion qualityVersion = IndexVersionUtils.randomVersionBetween(
            IndexVersions.DISK_BBQ_ES950_AUTO_CALIBRATE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.DISK_BBQ_AUTO_CALIBRATE_DEFAULT_ISO_SIZING)
        );
        IndexVersion isoSizingVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.DISK_BBQ_AUTO_CALIBRATE_DEFAULT_ISO_SIZING);
        List<Tuple<IndexVersion, IvfAutoCalibrationProfile>> testCases = List.of(
            Tuple.tuple(qualityVersion, IvfAutoCalibrationProfile.QUALITY),
            Tuple.tuple(isoSizingVersion, IvfAutoCalibrationProfile.ISO_SIZING)
        );

        for (Tuple<IndexVersion, IvfAutoCalibrationProfile> testCase : testCases) {
            IndexVersion version = testCase.v1();
            MapperService mapperService = createMapperService(version, EXPERIMENTAL_FEATURES_ENABLED, autoCalibrateMapping(true));
            assertEquals(
                testCase.v2(),
                getIndexOptions(mapperService, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class).autoCalibrationProfile()
            );

            // The stored mapping keeps the boolean, so recovery must resolve it against the same index version
            String mappingSource = mapperService.documentMapper().mappingSource().string();
            assertThat(mappingSource, containsString("\"auto_calibrate\":true"));
            MapperService recovered = new TestMapperServiceBuilder().indexVersion(version).settings(EXPERIMENTAL_FEATURES_ENABLED).build();
            merge(recovered, MapperService.MergeReason.MAPPING_RECOVERY, mappingSource);
            assertEquals(
                testCase.v2(),
                getIndexOptions(recovered, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class).autoCalibrationProfile()
            );
        }
    }

    public void testAutoCalibrateMappingUpdates() throws IOException {
        IndexVersion current = IndexVersion.current();
        IndexVersion oldVersion = IndexVersionUtils.randomVersionBetween(
            IndexVersions.DISK_BBQ_ES950_AUTO_CALIBRATE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.DISK_BBQ_AUTO_CALIBRATE_DEFAULT_ISO_SIZING)
        );

        assertAutoCalibrateUpdate(current, null, false, true);
        assertAutoCalibrateUpdate(current, false, "disabled", true);
        assertAutoCalibrateUpdate(current, true, "iso_sizing", true);
        assertAutoCalibrateUpdate(oldVersion, true, "quality", true);
        assertAutoCalibrateUpdate(oldVersion, true, "iso_sizing", false);
        assertAutoCalibrateUpdate(current, "quality", "iso_sizing", false);
        assertAutoCalibrateUpdate(current, "iso_sizing", "quality", false);
        assertAutoCalibrateUpdate(current, null, "quality", false);
    }

    private void assertAutoCalibrateUpdate(IndexVersion version, Object from, Object to, boolean accepted) throws IOException {
        String message = "index version [" + version + "], from [" + from + "] to [" + to + "]";
        MapperService mapperService = createMapperService(version, EXPERIMENTAL_FEATURES_ENABLED, autoCalibrateMapping(from));
        if (accepted) {
            merge(mapperService, autoCalibrateMapping(to));
            DenseVectorAutoCalibrate expected = DenseVectorAutoCalibrate.parse(to, version, f -> true, "field");
            assertEquals(
                message,
                expected.profile(),
                getIndexOptions(mapperService, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class).autoCalibrationProfile()
            );
            String expectedSource = to instanceof String ? "\"auto_calibrate\":\"" + to + "\"" : "\"auto_calibrate\":" + to;
            assertThat(message, mapperService.documentMapper().mappingSource().toString(), containsString(expectedSource));
        } else {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                message,
                () -> merge(mapperService, autoCalibrateMapping(to))
            );
            assertThat(message, e.getMessage(), containsString("Cannot update parameter [index_options]"));
        }
    }

    public void testAutoCalibrateProfilesRequireClusterFeature() throws IOException {
        final Supplier<MapperService> createMapperService = () -> new TestMapperServiceBuilder().settings(EXPERIMENTAL_FEATURES_ENABLED)
            .clusterSupportsFeature(f -> false)
            .build();

        {
            MapperService mapperService = createMapperService.get();
            XContentBuilder mapping = autoCalibrateMapping("quality");
            Exception e = expectThrows(MapperParsingException.class, () -> merge(mapperService, mapping));
            assertThat(e.getMessage(), containsString("'auto_calibrate' must be a boolean for field [field]"));

            // Recovery assumes all features are supported, so existing mappings that use profile names still load
            merge(mapperService, MapperService.MergeReason.MAPPING_RECOVERY, mapping);
            assertEquals(
                IvfAutoCalibrationProfile.QUALITY,
                getIndexOptions(mapperService, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class).autoCalibrationProfile()
            );
        }

        // Boolean values always work
        for (Object enabled : List.of(true, false, "true", "false")) {
            MapperService mapperService = createMapperService.get();
            merge(mapperService, autoCalibrateMapping(enabled));
            assertEquals(
                Booleans.parseBoolean(enabled.toString()),
                getIndexOptions(mapperService, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class).autoCalibrate()
            );
        }
    }

    public void testAutoCalibrateWithAsh() throws IOException {
        assumeTrue("ash requires a snapshot build", Build.current().isSnapshot());

        for (Object value : List.of(false, "disabled")) {
            MapperService mapperService = createMapperService(
                EXPERIMENTAL_FEATURES_ENABLED,
                autoCalibrateMapping(value, DenseVectorFieldMapper.BBQIVFIndexOptions.QuantizationType.ASH)
            );
            assertFalse(getIndexOptions(mapperService, "field", DenseVectorFieldMapper.BBQIVFIndexOptions.class).autoCalibrate());
        }

        for (Object value : List.of(true, "quality", "iso_sizing")) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    EXPERIMENTAL_FEATURES_ENABLED,
                    autoCalibrateMapping(value, DenseVectorFieldMapper.BBQIVFIndexOptions.QuantizationType.ASH)
                )
            );
            assertThat(e.getMessage(), containsString("'auto_calibrate' is not supported with 'quantization_type' 'ash'"));
        }
    }

    private static XContentBuilder autoCalibrateMapping(@Nullable Object autoCalibrate) throws IOException {
        return autoCalibrateMapping(autoCalibrate, DenseVectorFieldMapper.BBQIVFIndexOptions.QuantizationType.OSQ);
    }

    private static XContentBuilder autoCalibrateMapping(
        @Nullable Object autoCalibrate,
        DenseVectorFieldMapper.BBQIVFIndexOptions.QuantizationType quantizationType
    ) throws IOException {
        Map<String, Object> indexOptions = new HashMap<>();
        indexOptions.put("type", "bbq_disk");
        if (quantizationType != DenseVectorFieldMapper.BBQIVFIndexOptions.QuantizationType.OSQ) {
            indexOptions.put("quantization_type", quantizationType.toString());
        }
        if (autoCalibrate != null) {
            indexOptions.put("auto_calibrate", autoCalibrate);
        }
        return fieldMapping(
            b -> new DenseVectorMappingBuilder().dims(128)
                .index(true)
                .similarity(VectorSimilarity.DOT_PRODUCT)
                .indexOptions(indexOptions)
                .build(b)
        );
    }

    public void testRescoreVectorForNonQuantized() {
        for (String indexType : List.of("hnsw", "flat")) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> new DenseVectorMappingBuilder().index(true)
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 1.5f)))
                            .build(b)
                    )
                )
            );
            e.getMessage().contains("Mapping definition for [field] has unsupported parameters:");
        }
    }

    public void testRescoreVectorOldIndexVersion() {
        IndexVersion incompatibleVersion = randomFrom(
            IndexVersionUtils.randomVersionBetween(
                IndexVersionUtils.getLowestReadCompatibleVersion(),
                IndexVersionUtils.getPreviousVersion(IndexVersions.ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS_BACKPORT_8_X)
            ),
            IndexVersionUtils.randomVersionBetween(
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0,
                IndexVersionUtils.getPreviousVersion(IndexVersions.ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS)
            )
        );
        for (String indexType : List.of("int8_hnsw", "int8_flat", "int4_hnsw", "int4_flat", "bbq_hnsw", "bbq_flat")) {
            expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    incompatibleVersion,
                    fieldMapping(
                        b -> new DenseVectorMappingBuilder().index(true)
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 1.5f)))
                            .build(b)
                    )
                )
            );
        }
    }

    public void testRescoreZeroVectorOldIndexVersion() {
        IndexVersion incompatibleVersion = randomFrom(
            IndexVersionUtils.randomVersionBetween(
                IndexVersionUtils.getLowestReadCompatibleVersion(),
                IndexVersionUtils.getPreviousVersion(IndexVersions.RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS_BACKPORT_8_X)
            ),
            IndexVersionUtils.randomVersionBetween(
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0,
                IndexVersionUtils.getPreviousVersion(IndexVersions.RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS)
            )
        );
        for (String indexType : List.of("int8_hnsw", "int8_flat", "int4_hnsw", "int4_flat", "bbq_hnsw", "bbq_flat")) {
            expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    incompatibleVersion,
                    fieldMapping(
                        b -> new DenseVectorMappingBuilder().index(true)
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 0f)))
                            .build(b)
                    )
                )
            );
        }
    }

    public void testInvalidRescoreVector() {
        var base = new DenseVectorMappingBuilder().index(true);
        for (String indexType : List.of("int8_hnsw", "int8_flat", "int4_hnsw", "int4_flat", "bbq_hnsw", "bbq_flat")) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> base.clone()
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("foo", 1.5f)))
                            .build(b)
                    )
                )
            );
            e.getMessage().contains("Invalid rescore_vector value. Missing required field oversample");
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> base.clone()
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", "foo")))
                            .build(b)
                    )
                )
            );
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> base.clone()
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 0.1f)))
                            .build(b)
                    )
                )
            );
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> base.clone()
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of()))
                            .build(b)
                    )
                )
            );
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> base.clone()
                            .indexOptions(Map.of("type", indexType, DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 10.1f)))
                            .build(b)
                    )
                )
            );
        }
    }

    public void testDefaultOversampleValue() throws IOException {
        var base = new DenseVectorMappingBuilder().dims(128).index(true).similarity(VectorSimilarity.DOT_PRODUCT);
        {
            MapperService mapperService = createMapperService(
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "bbq_hnsw")).build(b))
            );

            DenseVectorFieldMapper.BBQHnswIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQHnswIndexOptions.class
            );
            assertEquals(3.0F, indexOptions.rescoreVector.oversample(), 0.0F);
        }
        {
            MapperService mapperService = createMapperService(
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "bbq_flat")).build(b))
            );

            DenseVectorFieldMapper.BBQFlatIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.BBQFlatIndexOptions.class
            );
            assertEquals(3.0F, indexOptions.rescoreVector.oversample(), 0.0F);
        }
        {
            MapperService mapperService = createMapperService(
                fieldMapping(b -> base.clone().indexOptions(Map.of("type", "int8_hnsw")).build(b))
            );

            DenseVectorFieldMapper.Int8HnswIndexOptions indexOptions = getIndexOptions(
                mapperService,
                "field",
                DenseVectorFieldMapper.Int8HnswIndexOptions.class
            );
            assertNull(indexOptions.rescoreVector);
        }
    }

    public void testDims() {
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> new DenseVectorMappingBuilder().dims(0).build(b)))
            );
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [0]")
            );
        }
        // test max limit for non-indexed vectors
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> new DenseVectorMappingBuilder().dims(5000).build(b)))
            );
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [5000]")
            );
        }
        // test max limit for indexed vectors
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> new DenseVectorMappingBuilder().dims(5000).index(true).build(b)))
            );
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [5000]")
            );
        }
    }

    public void testMergeDims() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> new DenseVectorMappingBuilder().build(b));
        MapperService mapperService = createMapperService(mapping);

        mapping = fieldMapping(
            b -> new DenseVectorMappingBuilder().dims(dims)
                .index(true)
                .similarity(VectorSimilarity.COSINE)
                .indexOptions(Map.of("type", "int8_hnsw", "m", 16, "ef_construction", 100))
                .build(b)
        );
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public void testLargeDimsBit() throws IOException {
        createMapperService(
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(1024 * Byte.SIZE).elementType(ElementType.BIT).build(b))
        );
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).build(b)));

        testIndexedVector(VectorSimilarity.COSINE, mapper);
    }

    public void testDefaultElementTypeUnderVectordbDocumentIndexMode() throws Exception {
        Settings settings = new DenseVectorTestSettingsBuilder().indexMode(IndexMode.VECTORDB_DOCUMENT).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> new DenseVectorMappingBuilder().dims(8).build(b)));
        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertEquals(ElementType.BFLOAT16, mapper.fieldType().getElementType());
    }

    public void testExplicitElementTypeOverridesVectordbDocumentModeDefault() throws Exception {
        Settings settings = new DenseVectorTestSettingsBuilder().indexMode(IndexMode.VECTORDB_DOCUMENT).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(8).elementType(ElementType.FLOAT).build(b))
        );
        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertEquals(ElementType.FLOAT, mapper.fieldType().getElementType());
    }

    public void testDefaultsUnderVectordbColumnarIndexMode() throws Exception {
        assumeTrue("vectordb_columnar index mode requires snapshot build", IndexMode.VECTORDB_COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = new DenseVectorTestSettingsBuilder().indexMode(IndexMode.VECTORDB_COLUMNAR).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> new DenseVectorMappingBuilder().dims(8).build(b)));
        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertEquals(ElementType.BFLOAT16, mapper.fieldType().getElementType());
        assertTrue(mapper.fieldType().isSearchable());
    }

    public void testExplicitElementTypeOverridesVectordbColumnarModeDefault() throws Exception {
        assumeTrue("vectordb_columnar index mode requires snapshot build", IndexMode.VECTORDB_COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = new DenseVectorTestSettingsBuilder().indexMode(IndexMode.VECTORDB_COLUMNAR).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(8).elementType(ElementType.FLOAT).build(b))
        );
        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertEquals(ElementType.FLOAT, mapper.fieldType().getElementType());
        assertTrue(mapper.fieldType().isSearchable());
    }

    public void testIndexedVector() throws Exception {
        VectorSimilarity similarity = RandomPicks.randomFrom(random(), VectorSimilarity.values());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).similarity(similarity).build(b))
        );

        testIndexedVector(similarity, mapper);
    }

    private void testIndexedVector(VectorSimilarity similarity, DocumentMapper mapper) throws Exception {

        float[] vector = { -0.5f, 0.5f, 0.7071f };
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(KnnFloatVectorField.class));

        KnnFloatVectorField vectorField = (KnnFloatVectorField) fields.get(0);
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue(), 0.001f);
        assertEquals(
            similarity.vectorSimilarityFunction(IndexVersion.current(), ElementType.FLOAT),
            vectorField.fieldType().vectorSimilarityFunction()
        );
    }

    public void testNonIndexedVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(false).build(b)));

        float[] validVector = { -12.1f, 100.7f, -4 };
        double dotProduct = 0.0f;
        for (float value : validVector) {
            dotProduct += value * value;
        }
        float expectedMagnitude = (float) Math.sqrt(dotProduct);
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", validVector)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields.get(0).binaryValue();
        float[] decodedValues = decodeDenseVector(IndexVersion.current(), vectorBR);
        float decodedMagnitude = VectorEncoderDecoder.decodeMagnitude(IndexVersion.current(), vectorBR);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.001f);
        assertArrayEquals("Decoded dense vector values is not equal to the indexed one.", validVector, decodedValues, 0.001f);
    }

    /**
     * A {@code bfloat16} vector with {@code index: false} is stored in binary doc values, whose byte order is chosen by index
     * version - see {@link DenseVectorFieldMapper#LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION}. The decode path must apply the same
     * rule, otherwise every component reads back byte-swapped on indices created before that version.
     */
    public void testNonIndexedBFloat16Vector() throws Exception {
        // Every component is exactly representable in bfloat16, so the assertions can be exact and the byte order unambiguous
        float[] vector = { 1.5f, 2.0f, -3.25f };
        float expectedMagnitude = (float) Math.sqrt(1.5f * 1.5f + 2.0f * 2.0f + 3.25f * 3.25f);

        for (IndexVersion indexVersion : List.of(
            IndexVersionUtils.randomVersionBetween(
                IndexVersionUtils.getLowestWriteCompatibleVersion(),
                IndexVersionUtils.getPreviousVersion(DenseVectorFieldMapper.LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)
            ),
            IndexVersionUtils.randomVersionBetween(DenseVectorFieldMapper.LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION, IndexVersion.current())
        )) {
            String message = "index version [" + indexVersion + "]";
            DocumentMapper mapper = createDocumentMapper(
                indexVersion,
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(vector.length).index(false).elementType(ElementType.BFLOAT16).build(b)
                )
            );
            ParsedDocument doc = mapper.parse(source(b -> b.array("field", vector)));

            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertThat(message, fields, hasSize(1));
            assertThat(message, fields.get(0), instanceOf(BinaryDocValuesField.class));

            BytesRef vectorBR = fields.get(0).binaryValue();
            float[] decoded = new float[vector.length];
            VectorEncoderDecoder.decodeBFloat16DenseVector(indexVersion, vectorBR, decoded);
            assertArrayEquals(message, vector, decoded, 0f);
            assertThat(message, VectorEncoderDecoder.decodeMagnitude(indexVersion, vectorBR), equalTo(expectedMagnitude));
        }
    }

    public void testIndexedByteVector() throws Exception {
        VectorSimilarity similarity = RandomPicks.randomFrom(random(), VectorSimilarity.values());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(3).index(true).similarity(similarity).elementType(ElementType.BYTE).build(b)
            )
        );

        byte[] vector = { (byte) -1, (byte) 1, (byte) 127 };
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(KnnByteVectorField.class));

        KnnByteVectorField vectorField = (KnnByteVectorField) fields.get(0);
        vectorField.vectorValue();
        assertArrayEquals(
            "Parsed vector is not equal to original.",
            new byte[] { (byte) -1, (byte) 1, (byte) 127 },
            vectorField.vectorValue()
        );
        assertEquals(
            similarity.vectorSimilarityFunction(IndexVersion.current(), ElementType.BYTE),
            vectorField.fieldType().vectorSimilarityFunction()
        );
    }

    public void testDotProductWithInvalidNorm() throws Exception {
        var base = new DenseVectorMappingBuilder().index(true).similarity(VectorSimilarity.DOT_PRODUCT);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> base.clone().dims(3).build(b)));
        float[] vector = { -12.1f, 2.7f, -4 };
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", vector)))
        );
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "The [dot_product] similarity can only be used with unit-length vectors. Preview of invalid vector: [-12.1, 2.7, -4.0]"
            )
        );

        DocumentMapper mapperWithLargerDim = createDocumentMapper(fieldMapping(b -> base.clone().dims(6).build(b)));
        float[] largerVector = { -12.1f, 2.7f, -4, 1.05f, 10.0f, 29.9f };
        e = expectThrows(DocumentParsingException.class, () -> mapperWithLargerDim.parse(source(b -> b.array("field", largerVector))));
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "The [dot_product] similarity can only be used with unit-length vectors. "
                    + "Preview of invalid vector: [-12.1, 2.7, -4.0, 1.05, 10.0, ...]"
            )
        );
    }

    public void testCosineWithZeroVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(true).similarity(VectorSimilarity.COSINE).build(b))
        );
        float[] vector = { -0.0f, 0.0f, 0.0f };
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", vector)))
        );
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "The [cosine] similarity does not support vectors with zero magnitude. Preview of invalid vector: [-0.0, 0.0, 0.0]"
            )
        );
    }

    public void testCosineWithZeroByteVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(3)
                    .index(true)
                    .similarity(VectorSimilarity.COSINE)
                    .elementType(ElementType.BYTE)
                    .build(b)
            )
        );
        float[] vector = { -0.0f, 0.0f, 0.0f };
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", vector)))
        );
        assertNotNull(e.getCause());
        assertThat(
            e.getCause().getMessage(),
            containsString("The [cosine] similarity does not support vectors with zero magnitude. Preview of invalid vector: [0, 0, 0]")
        );
    }

    public void testMaxInnerProductWithValidNorm() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(true).similarity(VectorSimilarity.MAX_INNER_PRODUCT).build(b))
        );
        float[] vector = { -12.1f, 2.7f, -4 };
        // Shouldn't throw
        mapper.parse(source(b -> b.array("field", vector)));
    }

    public void testWithExtremeFloatVector() throws Exception {
        for (VectorSimilarity vs : List.of(VectorSimilarity.COSINE, VectorSimilarity.DOT_PRODUCT, VectorSimilarity.COSINE)) {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(true).similarity(vs).build(b))
            );
            float[] vector = { 0.07247924f, -4.310546E-11f, -1.7255947E30f };
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.array("field", vector)))
            );
            assertNotNull(e.getCause());
            assertThat(
                e.getCause().getMessage(),
                containsString(
                    "NaN or Infinite magnitude detected, this usually means the vector values are too extreme to fit within a float."
                )
            );
        }
    }

    public void testInvalidParameters() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(false).similarity(VectorSimilarity.L2_NORM).build(b))
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Field [similarity] can only be specified for a field of type [dense_vector] when it is indexed")
        );

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(3)
                        .index(false)
                        .indexOptions(Map.of("type", "hnsw", "m", 5, "ef_construction", 100))
                        .build(b)
                )
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Field [index_options] can only be specified for a field of type [dense_vector] when it is indexed")
        );

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(3)
                        .index(true)
                        .similarity(VectorSimilarity.L2_NORM)
                        .indexOptions(Map.of())
                        .build(b)
                )
            )
        );
        assertThat(e.getMessage(), containsString("[index_options] requires field [type] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).additionalParams(fb -> fb.field("element_type", "foo")).build(b))
            )
        );
        assertThat(e.getMessage(), containsString("invalid element_type [foo]; available types are "));
        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(3)
                        .index(true)
                        .similarity(VectorSimilarity.L2_NORM)
                        .indexOptions(Map.of("type", "hnsw", "foo", Map.of()))
                        .build(b)
                )
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: Mapping definition for [field] has unsupported parameters:  [foo : {}]")
        );
        List<String> floatOnlyQuantizations = new ArrayList<>(
            Arrays.asList("int4_hnsw", "int8_hnsw", "int8_flat", "int4_flat", "bbq_hnsw", "bbq_flat")
        );
        for (String quantizationKind : floatOnlyQuantizations) {
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> new DenseVectorMappingBuilder().dims(64)
                            .index(true)
                            .similarity(VectorSimilarity.L2_NORM)
                            .elementType(ElementType.BYTE)
                            .indexOptions(Map.of("type", quantizationKind))
                            .build(b)
                    )
                )
            );
            assertThat(
                e.getMessage(),
                containsString("Failed to parse mapping: [element_type] cannot be [byte] when using index type [" + quantizationKind + "]")
            );
        }
    }

    public void testInvalidParametersBeforeIndexedByDefault() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION,
                fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(true).build(b))
            )
        );

        assertThat(e.getMessage(), containsString("Field [index] requires field [similarity] to be configured and not null"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION,
                fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).similarity(VectorSimilarity.COSINE).build(b))
            )
        );

        assertThat(
            e.getMessage(),
            containsString("Field [similarity] can only be specified for a field of type [dense_vector] when it is indexed")
        );

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION,
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(3)
                        .indexOptions(Map.of("type", "hnsw", "m", 200, "ef_construction", 20))
                        .build(b)
                )
            )
        );

        assertThat(
            e.getMessage(),
            containsString("Field [index_options] can only be specified for a field of type [dense_vector] when it is indexed")
        );
    }

    public void testDefaultParamsBeforeIndexByDefault() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION,
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).build(b))
        );
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) documentMapper.mappers().getMapper("field");
        DenseVectorFieldType denseVectorFieldType = denseVectorFieldMapper.fieldType();

        assertTrue(denseVectorFieldType.indexType().hasOnlyDocValues());
        assertNull(denseVectorFieldType.getSimilarity());
    }

    public void testParamsBeforeIndexByDefault() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION,
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).index(true).similarity(VectorSimilarity.DOT_PRODUCT).build(b))
        );
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) documentMapper.mappers().getMapper("field");
        DenseVectorFieldType denseVectorFieldType = denseVectorFieldMapper.fieldType();

        assertTrue(denseVectorFieldType.indexType().hasVectors());
        assertEquals(VectorSimilarity.DOT_PRODUCT, denseVectorFieldType.getSimilarity());
    }

    public void testDefaultParamsIndexByDefault() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(fieldMapping(b -> new DenseVectorMappingBuilder().dims(3).build(b)));
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) documentMapper.mappers().getMapper("field");
        DenseVectorFieldType denseVectorFieldType = denseVectorFieldMapper.fieldType();

        assertTrue(denseVectorFieldType.indexType().hasVectors());
        assertEquals(VectorSimilarity.COSINE, denseVectorFieldType.getSimilarity());
    }

    public void testDefaultIndexOptions() throws IOException {
        for (int i = 0; i < 100; i++) {
            // Pick a random index version from one of three eras that each produce different default index options
            int era = randomIntBetween(0, 3);
            IndexVersion indexVersion = switch (era) {
                case 0 -> IndexVersionUtils.randomVersionBetween(
                    IndexVersionUtils.getLowestReadCompatibleVersion(),
                    IndexVersionUtils.getPreviousVersion(DenseVectorFieldMapper.DEFAULT_TO_INT8)
                );
                case 1 -> IndexVersionUtils.randomVersionBetween(
                    DenseVectorFieldMapper.DEFAULT_TO_INT8,
                    IndexVersionUtils.getPreviousVersion(DenseVectorFieldMapper.DEFAULT_TO_BBQ)
                );
                case 2 -> IndexVersionUtils.randomVersionBetween(
                    DenseVectorFieldMapper.DEFAULT_TO_BBQ,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.DENSE_VECTOR_BFLOAT16_DEFAULT_INDEX_OPTIONS)
                );
                case 3 -> IndexVersionUtils.randomVersionBetween(
                    IndexVersions.DENSE_VECTOR_BFLOAT16_DEFAULT_INDEX_OPTIONS,
                    IndexVersion.current()
                );
                default -> throw new AssertionError("Unexpected value: " + era);
            };

            boolean defaultInt8Hnsw = indexVersion.onOrAfter(DenseVectorFieldMapper.DEFAULT_TO_INT8);
            boolean defaultBBQHnsw = indexVersion.onOrAfter(DenseVectorFieldMapper.DEFAULT_TO_BBQ);

            final ElementType elementType = randomFrom(ElementType.values());
            final int dims = DenseVectorFieldMapperTestUtils.randomCompatibleDimensions(elementType, 512);
            final VectorSimilarity similarity = randomFrom(
                DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType)
                    .stream()
                    .map(SimilarityMeasure::vectorSimilarity)
                    .toList()
            );

            MapperService mapperService = createMapperService(
                indexVersion,
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(dims).index(true).similarity(similarity).elementType(elementType).build(b)
                )
            );

            DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
            DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions = mapper.fieldType().getIndexOptions();

            if (DenseVectorFieldMapperTestUtils.elementTypesWithDefaultIndexOptions(indexVersion).contains(elementType) == false) {
                assertNull(indexOptions);
            } else if (defaultBBQHnsw && dims >= DenseVectorFieldMapper.BBQ_DIMS_DEFAULT_THRESHOLD) {
                assertThat(indexOptions, instanceOf(DenseVectorFieldMapper.BBQHnswIndexOptions.class));
            } else if (defaultInt8Hnsw) {
                // INT8 era, or BBQ era with dims below the BBQ threshold
                assertThat(indexOptions, instanceOf(DenseVectorFieldMapper.Int8HnswIndexOptions.class));
            } else {
                assertNull(indexOptions);
            }
        }
    }

    public void testValidateOnBuild() {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        int dimensions = randomIntBetween(64, 1024);
        // Build a dense vector field mapper with float element type, which will trigger int8 HNSW index options
        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder(
            "test",
            IndexVersion.current(),
            IndexMode.STANDARD,
            false,
            false,
            List.of(),
            false
        ).elementType(ElementType.FLOAT).dimensions(dimensions).build(context);

        // Change the element type to byte, which is incompatible with int8 HNSW index options
        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(ElementType.BYTE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.build(context));
        assertThat(
            e.getMessage(),
            containsString(
                dimensions >= DenseVectorFieldMapper.BBQ_DIMS_DEFAULT_THRESHOLD
                    ? "[element_type] cannot be [byte] when using index type [bbq_hnsw]"
                    : "[element_type] cannot be [byte] when using index type [int8_hnsw]"
            )
        );
    }

    private static float[] decodeDenseVector(IndexVersion indexVersion, BytesRef encodedVector) {
        int dimCount = VectorEncoderDecoder.denseVectorLength(indexVersion, encodedVector);
        float[] vector = new float[dimCount];
        VectorEncoderDecoder.decodeDenseVector(indexVersion, encodedVector, vector);
        return vector;
    }

    public void testDocumentsWithIncorrectDims() throws Exception {
        for (boolean index : Arrays.asList(false, true)) {
            int dims = 3;
            var mapping = new DenseVectorMappingBuilder().dims(dims).index(index);
            if (index) {
                mapping.similarity(VectorSimilarity.DOT_PRODUCT);
            }
            XContentBuilder fieldMapping = fieldMapping(mapping::build);

            DocumentMapper mapper = createDocumentMapper(fieldMapping);

            // test that error is thrown when a document has number of dims more than defined in the mapping
            float[] invalidVector = new float[dims + 1];
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.array("field", invalidVector)))
            );
            assertThat(e.getCause().getMessage(), containsString("has more dimensions than defined in the mapping [3]"));

            // test that error is thrown when a document has number of dims less than defined in the mapping
            float[] invalidVector2 = new float[dims - 1];
            DocumentParsingException e2 = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.array("field", invalidVector2)))
            );
            assertThat(
                e2.getCause().getMessage(),
                containsString("has a different number of dimensions [2] than defined in the mapping [3]")
            );
        }
    }

    private record InvalidEncodedVector(String encoded, String expectedMessage) {}

    private static List<InvalidEncodedVector> invalidEncodedVectors(ElementType elementType, int dims) {
        return switch (elementType) {
            case BYTE, BIT -> List.of(
                // Not valid base64 and not valid hex
                new InvalidEncodedVector("garbage!", "value must be a valid base64 or hex string"),
                // Valid hex of wrong length; also valid base64 but wrong length — hex message wins
                new InvalidEncodedVector(
                    "807f0a0b",
                    "hex-decoded vector has a different number of dimensions [" + elementType.dims(4) + "] than the expected [" + dims + "]"
                ),
                // Valid base64 of wrong byte count; leading '/' is not a hex digit so not misread as hex
                new InvalidEncodedVector(
                    "/wAAAA==",
                    "Base64 decoded vector byte length [4] does not match the expected length of ["
                        + elementType.vectorLength(dims)
                        + "] for dimension count ["
                        + dims
                        + "]"
                )
            );
            case FLOAT, BFLOAT16 -> List.of(
                // Not valid base64; hex is disabled for float fields
                new InvalidEncodedVector("not-valid-base64!!!", "value must be a valid base64 string"),
                // '807f0a' is hex-looking but hex is disabled; Java decodes it as base64 to 4 bytes (not 12 or 6)
                new InvalidEncodedVector(
                    "807f0a",
                    "Base64 decoded vector byte length [4] does not match the expected length of [12] or [6] for dimension count ["
                        + dims
                        + "]"
                ),
                // Valid base64 of 8 bytes; accepted lengths are 12 (float32) or 6 (bfloat16)
                new InvalidEncodedVector(
                    "PczMzT5MzM0=",
                    "Base64 decoded vector byte length [8] does not match the expected length of [12] or [6] for dimension count ["
                        + dims
                        + "]"
                )
            );
        };
    }

    public void testDocumentsWithInvalidEncodedVectors() throws Exception {
        for (ElementType elementType : List.of(ElementType.BYTE, ElementType.BIT, ElementType.FLOAT, ElementType.BFLOAT16)) {
            int dims = elementType.dims(3);
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(dims)
                        .index(true)
                        .similarity(VectorSimilarity.L2_NORM)
                        .elementType(elementType)
                        .build(b)
                )
            );
            for (InvalidEncodedVector invalid : invalidEncodedVectors(elementType, dims)) {
                DocumentParsingException e = expectThrows(
                    DocumentParsingException.class,
                    () -> mapper.parse(source(b -> b.field("field", invalid.encoded())))
                );
                assertThat(e.getCause().getMessage(), containsString(invalid.expectedMessage()));
            }
        }
    }

    public void testCosineDenseVectorValues() throws IOException {
        final int dims = randomIntBetween(64, 2048);
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(dims).index(true).similarity(similarity).build(b))
        );
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat() * randomIntBetween(1, 10);
        }
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(KnnFloatVectorField.class));
        KnnFloatVectorField vectorField = (KnnFloatVectorField) fields.get(0);
        // Cosine vectors are now normalized
        VectorUtil.l2normalize(vector);
        assertArrayEquals("Parsed vector is not equal to normalized original.", vector, vectorField.vectorValue(), 0.001f);
    }

    public void testCosineDenseVectorValuesOlderIndexVersions() throws IOException {
        final int dims = randomIntBetween(64, 2048);
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        DocumentMapper mapper = createDocumentMapper(
            IndexVersionUtils.randomVersionBetween(IndexVersions.V_8_0_0, IndexVersions.NEW_SPARSE_VECTOR),
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(dims).index(true).similarity(similarity).build(b))
        );
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat() * randomIntBetween(1, 10);
        }
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(KnnFloatVectorField.class));
        KnnFloatVectorField vectorField = (KnnFloatVectorField) fields.get(0);
        // Cosine vectors are now normalized
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue(), 0.001f);
    }

    /**
     * Test that max dimensions limit for float dense_vector field
     * is 4096 as defined by {@link DenseVectorFieldMapper#MAX_DIMS_COUNT}
     */
    public void testMaxDimsFloatVector() throws IOException {
        final int dims = 4096;
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> new DenseVectorMappingBuilder().dims(dims).index(true).similarity(similarity).build(b))
        );

        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(KnnFloatVectorField.class));
        KnnFloatVectorField vectorField = (KnnFloatVectorField) fields.get(0);
        assertEquals(dims, vectorField.fieldType().vectorDimension());
        assertEquals(VectorEncoding.FLOAT32, vectorField.fieldType().vectorEncoding());
        assertEquals(VectorSimilarityFunction.DOT_PRODUCT, vectorField.fieldType().vectorSimilarityFunction());
        // Cosine vectors are now normalized
        VectorUtil.l2normalize(vector);
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue(), 0.001f);
    }

    /**
     * Test that max dimensions limit for byte dense_vector field
     * is 4096 as defined by {@link KnnByteVectorField}
     */
    public void testMaxDimsByteVector() throws IOException {
        final int dims = 4096;
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        ;
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(dims).index(true).similarity(similarity).elementType(ElementType.BYTE).build(b)
            )
        );

        byte[] vector = new byte[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomByte();
        }
        ParsedDocument doc1 = mapper.parse(source(b -> b.array("field", vector)));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(KnnByteVectorField.class));
        KnnByteVectorField vectorField = (KnnByteVectorField) fields.get(0);
        assertEquals(dims, vectorField.fieldType().vectorDimension());
        assertEquals(VectorEncoding.BYTE, vectorField.fieldType().vectorEncoding());
        assertEquals(
            similarity.vectorSimilarityFunction(IndexVersion.current(), ElementType.BYTE),
            vectorField.fieldType().vectorSimilarityFunction()
        );
        assertArrayEquals("Parsed vector is not equal to original.", vector, vectorField.vectorValue());
    }

    public void testVectorSimilarity() {
        assertEquals(
            VectorSimilarityFunction.COSINE,
            VectorSimilarity.COSINE.vectorSimilarityFunction(IndexVersion.current(), ElementType.BYTE)
        );
        assertEquals(
            VectorSimilarityFunction.COSINE,
            VectorSimilarity.COSINE.vectorSimilarityFunction(
                IndexVersionUtils.randomVersionBetween(
                    IndexVersions.V_8_0_0,
                    IndexVersionUtils.getPreviousVersion(DenseVectorFieldMapper.NORMALIZE_COSINE)
                ),
                ElementType.FLOAT
            )
        );
        assertEquals(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarity.COSINE.vectorSimilarityFunction(
                IndexVersionUtils.randomVersionBetween(DenseVectorFieldMapper.NORMALIZE_COSINE, IndexVersion.current()),
                ElementType.FLOAT
            )
        );
        // the default similarity function is raw scoring: never apply the NORMALIZE_COSINE optimization
        assertEquals(VectorSimilarityFunction.COSINE, VectorSimilarity.COSINE.defaultVectorSimilarityFunction());
        assertEquals(
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarity.L2_NORM.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.BYTE)
        );
        assertEquals(
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarity.L2_NORM.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.FLOAT)
        );
        assertEquals(VectorSimilarityFunction.EUCLIDEAN, VectorSimilarity.L2_NORM.defaultVectorSimilarityFunction());
        assertEquals(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarity.DOT_PRODUCT.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.BYTE)
        );
        assertEquals(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarity.DOT_PRODUCT.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.FLOAT)
        );
        assertEquals(VectorSimilarityFunction.DOT_PRODUCT, VectorSimilarity.DOT_PRODUCT.defaultVectorSimilarityFunction());
        assertEquals(
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
            VectorSimilarity.MAX_INNER_PRODUCT.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.BYTE)
        );
        assertEquals(
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
            VectorSimilarity.MAX_INNER_PRODUCT.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.FLOAT)
        );
        assertEquals(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT, VectorSimilarity.MAX_INNER_PRODUCT.defaultVectorSimilarityFunction());
    }

    @Override
    protected void assertFetchMany(MapperService mapperService, String field, Object value, String format, int count) throws IOException {
        assumeFalse("Dense vectors currently don't support multiple values in the same field", false);
    }

    /**
     * Dense vectors don't support doc values or string representation (for doc value parser/fetching).
     * We may eventually support that, but until then, we only verify that the parsing and fields fetching matches the provided value object
     */
    @Override
    protected void assertFetch(MapperService mapperService, String field, Object value, String format) throws IOException {
        MappedFieldType ft = mapperService.fieldType(field);
        MappedFieldType.FielddataOperation fdt = MappedFieldType.FielddataOperation.SEARCH;
        SourceToParse source = source(b -> b.field(ft.name(), value));
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft, fdt)).thenAnswer(inv -> fieldDataLookup(mapperService).apply(ft, () -> {
            throw new UnsupportedOperationException();
        }, fdt));
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromLookup(
                mapperService.mappingLookup(),
                null,
                mapperService.getMapperMetrics().sourceFieldMetrics(),
                null
            ).getSource(ir.leaves().get(0), 0);
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromNative = nativeFetcher.fetchValues(s, 0, new ArrayList<>());
            DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) ft;
            switch (denseVectorFieldType.getElementType()) {
                case BYTE -> {
                    assumeFalse("byte element type testing not currently added", false);
                }
                case FLOAT -> {
                    float[] fetchedFloats = new float[denseVectorFieldType.getVectorDimensions()];
                    int i = 0;
                    for (var f : fromNative) {
                        assert f instanceof Number;
                        fetchedFloats[i++] = ((Number) f).floatValue();
                    }
                    assertThat("fetching " + value, fetchedFloats, equalTo(value));
                }
            }
        });
    }

    @Override
    // TODO: add `byte` element_type tests
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        boolean index = randomBoolean();
        var mapping = new DenseVectorMappingBuilder().dims(randomIntBetween(2, 4096)).elementType(ElementType.FLOAT);
        if (index) {
            mapping.index(true).similarity(randomFrom(VectorSimilarity.values()));
        }
        mapping.build(b);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) ft;
        return switch (vectorFieldType.getElementType()) {
            case BYTE -> randomByteArrayOfLength(vectorFieldType.getVectorDimensions());
            case FLOAT, BFLOAT16 -> randomNormalizedVector(vectorFieldType.getVectorDimensions());
            case BIT -> randomByteArrayOfLength(vectorFieldType.getVectorDimensions() / 8);
        };
    }

    public void testCannotBeUsedInMultifields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("vectors");
            minimalMapping(b);
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [vectors] of type [dense_vector] can't be used in multifields"));
    }

    public void testByteVectorIndexBoundaries() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(3)
                    .index(true)
                    .similarity(VectorSimilarity.COSINE)
                    .elementType(ElementType.BYTE)
                    .build(b)
            )
        );

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", new float[] { 128, 0, 0 })))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [128] at dim [0];")
        );

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.array("field", new float[] { 18.2f, 0, 0 }))));
        assertThat(
            e.getCause().getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [18.2] at dim [0];")
        );

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", new float[] { 0.0f, 0.0f, -129.0f })))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [-129] at dim [2];")
        );
    }

    public void testByteVectorQueryBoundaries() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(3)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .elementType(ElementType.BYTE)
                    .indexOptions(Map.of("type", "hnsw", "m", 3, "ef_construction", 10))
                    .build(b)
            )
        );

        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 128, 0, 0 }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 0.0f, 0f, -129.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [2];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 0.0f, 0.5f, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [1];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 0, 0.0f, -0.25f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [-0.25] at dim [2];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.NaN, 0f, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(e.getMessage(), containsString("element_type [byte] vectors do not support NaN values but found [NaN] at dim [0];"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.POSITIVE_INFINITY, 0f, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors do not support infinite values but found [Infinity] at dim [0];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 0, Float.NEGATIVE_INFINITY, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors do not support infinite values but found [-Infinity] at dim [1];")
        );
    }

    public void testFloatVectorQueryBoundaries() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(3)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .elementType(ElementType.FLOAT)
                    .indexOptions(Map.of("type", "hnsw", "m", 3, "ef_construction", 10))
                    .build(b)
            )
        );

        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.NaN, 0f, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(e.getMessage(), containsString("element_type [float] vectors do not support NaN values but found [NaN] at dim [0];"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.POSITIVE_INFINITY, 0f, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [float] vectors do not support infinite values but found [Infinity] at dim [0];")
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 0, Float.NEGATIVE_INFINITY, 0.0f }),
                3,
                3,
                10f,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values()),
                randomBoolean()
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [float] vectors do not support infinite values but found [-Infinity] at dim [1];")
        );
    }

    public void testKnnVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        boolean setM = randomBoolean();
        boolean setEfConstruction = randomBoolean();
        Map<String, Object> indexOptions = new HashMap<>();
        indexOptions.put("type", "hnsw");
        if (setM) {
            indexOptions.put("m", m);
        }
        if (setEfConstruction) {
            indexOptions.put("ef_construction", efConstruction);
        }
        MapperService mapperService = createMapperService(
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_4_0),
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(dims)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .indexOptions(indexOptions)
                    .build(b)
            )
        );
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
        Codec codec = codecService.codec("default");
        assertThat(codec, instanceOf(PerFieldMapperCodec.class));
        KnnVectorsFormat knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");

        assertThat(
            knnVectorsFormat,
            hasToString(
                allOf(
                    startsWith(
                        "ES93HnswVectorsFormat(name=ES93HnswVectorsFormat, maxConn="
                            + (setM ? m : DEFAULT_MAX_CONN)
                            + ", beamWidth="
                            + (setEfConstruction ? efConstruction : DEFAULT_BEAM_WIDTH)
                            + ", hnswGraphThreshold="
                            + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD
                            + ", flatVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat"
                    ),
                    anyOf(
                        containsString(
                            "flatVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                + "ES93GenericFlatVectorScorer(delegate=PanamaFlatVectorScorer())), useDirectIO=false, onDiskMerge=false)"
                        ),
                        containsString(
                            "flatVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                + "ES93GenericFlatVectorScorer(delegate=ESDefaultFlatVectorScorer(delegate="
                                + "Lucene99MemorySegmentFlatVectorsScorer()))), useDirectIO=false, onDiskMerge=false)"
                        )
                    )
                )
            )
        );
    }

    public void testConfidenceIntervalDeprecationOnLatestIndexVersion() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            IndexVersions.UPGRADE_TO_LUCENE_10_4_0,
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(6)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .indexOptions(Map.of("type", "int8_hnsw", "confidence_interval", 0.95f))
                    .build(b)
            )
        );
        assertTrue(mapper.mappingSource().string().contains("\"confidence_interval\":0.95"));
        assertWarnings(
            "Parameter [confidence_interval] in [index_options] for dense_vector field "
                + "[field] is deprecated and will be removed in a future version"
        );
    }

    public void testConfidenceIntervalNoDeprecationBeforeLatestIndexVersion() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_4_0),
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(6)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .indexOptions(Map.of("type", "int8_hnsw", "confidence_interval", 0.95f))
                    .build(b)
            )
        );
        assertTrue(mapper.mappingSource().string().contains("\"confidence_interval\":0.95"));
        assertWarnings();
    }

    public void testKnnQuantizedFlatVectorsFormat() throws IOException {
        for (String quantizedFlatFormat : new String[] { "int8_flat", "int4_flat" }) {
            MapperService mapperService = createMapperService(
                IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_4_0),
                fieldMapping(
                    b -> new DenseVectorMappingBuilder().dims(dims)
                        .index(true)
                        .similarity(VectorSimilarity.DOT_PRODUCT)
                        .indexOptions(Map.of("type", quantizedFlatFormat))
                        .build(b)
                )
            );
            CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
            Codec codec = codecService.codec("default");
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            KnnVectorsFormat knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
            VectorScorerFactory factory = ESVectorizationProvider.getInstance().getVectorScorerFactory();
            String encoding = quantizedFlatFormat.equals("int4_flat") ? "PACKED_NIBBLE" : "SEVEN_BIT";
            assertThat(
                knnVectorsFormat,
                hasToString(
                    allOf(
                        containsString("ES94ScalarQuantizedVectorsFormat(name=ES94ScalarQuantizedVectorsFormat"),
                        containsString("encoding=" + encoding),
                        containsString("flatVectorScorer=ESQuantizedFlatVectorsScorer("),
                        containsString("factory=" + factory),
                        anyOf(
                            containsString(
                                "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                    + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                    + "ES93GenericFlatVectorScorer(delegate=PanamaFlatVectorScorer()))"
                                    + ", useDirectIO=false, onDiskMerge=false)"
                            ),
                            containsString(
                                "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                    + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                    + "ES93GenericFlatVectorScorer(delegate=ESDefaultFlatVectorScorer(delegate="
                                    + "Lucene99MemorySegmentFlatVectorsScorer()))), useDirectIO=false, onDiskMerge=false)"
                            )
                        )
                    )
                )
            );
        }
    }

    public void testKnnQuantizedHNSWVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        MapperService mapperService = createMapperService(
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_4_0),
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(dims)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .indexOptions(Map.of("type", "int8_hnsw", "m", m, "ef_construction", efConstruction))
                    .build(b)
            )
        );
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
        Codec codec = codecService.codec("default");
        assertThat(codec, instanceOf(PerFieldMapperCodec.class));
        KnnVectorsFormat knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        VectorScorerFactory factory = ESVectorizationProvider.getInstance().getVectorScorerFactory();
        assertThat(
            knnVectorsFormat,
            hasToString(
                allOf(
                    startsWith(
                        "ES94HnswScalarQuantizedVectorsFormat(name=ES94HnswScalarQuantizedVectorsFormat, maxConn="
                            + m
                            + ", beamWidth="
                            + efConstruction
                            + ", hnswGraphThreshold="
                            + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD
                            + ", flatVectorFormat=ES94ScalarQuantizedVectorsFormat(name=ES94ScalarQuantizedVectorsFormat"
                    ),
                    containsString("encoding=SEVEN_BIT"),
                    containsString("factory=" + factory),
                    anyOf(
                        containsString(
                            "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                + "ES93GenericFlatVectorScorer(delegate=PanamaFlatVectorScorer())), useDirectIO=false, onDiskMerge=false)"
                        ),
                        containsString(
                            "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                + "ES93GenericFlatVectorScorer(delegate=ESDefaultFlatVectorScorer(delegate="
                                + "Lucene99MemorySegmentFlatVectorsScorer()))), useDirectIO=false, onDiskMerge=false)"
                        )
                    )
                )
            )
        );
    }

    public void testKnnBBQHNSWVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        final int dims = randomIntBetween(64, 4096);
        MapperService mapperService = createMapperService(
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_4_0),
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(dims)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .indexOptions(Map.of("type", "bbq_hnsw", "m", m, "ef_construction", efConstruction))
                    .build(b)
            )
        );
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
        Codec codec = codecService.codec("default");
        assertThat(codec, instanceOf(PerFieldMapperCodec.class));
        KnnVectorsFormat knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        String expectedPrefix = "ES93HnswBinaryQuantizedVectorsFormat(name=ES93HnswBinaryQuantizedVectorsFormat, maxConn="
            + m
            + ", beamWidth="
            + efConstruction
            + ", hnswGraphThreshold="
            + ES93HnswBinaryQuantizedVectorsFormat.BBQ_HNSW_GRAPH_THRESHOLD
            + ", flatVectorFormat=ES93BinaryQuantizedVectorsFormat("
            + "name=ES93BinaryQuantizedVectorsFormat, "
            + "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat,"
            + " format=Lucene99FlatVectorsFormat";
        assertThat(knnVectorsFormat, hasToString(startsWith(expectedPrefix)));
    }

    public void testInvalidVectorDimensionsBBQ() {
        for (String quantizedFlatFormat : new String[] { "bbq_hnsw", "bbq_flat" }) {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> new DenseVectorMappingBuilder().dims(randomIntBetween(1, 63))
                            .index(true)
                            .similarity(VectorSimilarity.DOT_PRODUCT)
                            .elementType(ElementType.FLOAT)
                            .indexOptions(Map.of("type", quantizedFlatFormat))
                            .build(b)
                    )
                )
            );
            assertThat(e.getMessage(), containsString("does not support dimensions fewer than 64"));
        }
    }

    public void testKnnHalfByteQuantizedHNSWVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> new DenseVectorMappingBuilder().dims(dims)
                    .index(true)
                    .similarity(VectorSimilarity.DOT_PRODUCT)
                    .indexOptions(Map.of("type", "int4_hnsw", "m", m, "ef_construction", efConstruction))
                    .build(b)
            )
        );
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
        Codec codec = codecService.codec("default");
        assertThat(codec, instanceOf(PerFieldMapperCodec.class));
        KnnVectorsFormat knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        VectorScorerFactory factory = ESVectorizationProvider.getInstance().getVectorScorerFactory();
        assertThat(
            knnVectorsFormat,
            hasToString(
                allOf(
                    startsWith(
                        "ES94HnswScalarQuantizedVectorsFormat(name=ES94HnswScalarQuantizedVectorsFormat, maxConn="
                            + m
                            + ", beamWidth="
                            + efConstruction
                            + ", hnswGraphThreshold="
                            + ES93HnswVectorsFormat.HNSW_GRAPH_THRESHOLD
                            + ", flatVectorFormat=ES94ScalarQuantizedVectorsFormat(name=ES94ScalarQuantizedVectorsFormat"
                    ),
                    containsString("encoding=PACKED_NIBBLE"),
                    containsString("factory=" + factory),
                    anyOf(
                        containsString(
                            "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                + "ES93GenericFlatVectorScorer(delegate=PanamaFlatVectorScorer())), useDirectIO=false, onDiskMerge=false)"
                        ),
                        containsString(
                            "rawVectorFormat=ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format="
                                + "Lucene99FlatVectorsFormat(name=Lucene99FlatVectorsFormat, flatVectorScorer="
                                + "ES93GenericFlatVectorScorer(delegate=ESDefaultFlatVectorScorer(delegate="
                                + "Lucene99MemorySegmentFlatVectorsScorer()))), useDirectIO=false, onDiskMerge=false)"
                        )
                    )
                )
            )
        );
    }

    public void testInvalidVectorDimensions() {
        for (String quantizedFlatFormat : new String[] { "int4_hnsw", "int4_flat" }) {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> new DenseVectorMappingBuilder().dims(5)
                            .index(true)
                            .similarity(VectorSimilarity.DOT_PRODUCT)
                            .elementType(ElementType.FLOAT)
                            .indexOptions(Map.of("type", quantizedFlatFormat))
                            .build(b)
                    )
                )
            );
            assertThat(e.getMessage(), containsString("only supports even dimensions"));
        }
    }

    public void testPushingDownExecutorAndThreads() {
        TestDenseVectorIndexOptions testIndexOptions = new TestDenseVectorIndexOptions(
            new DenseVectorFieldMapper.HnswIndexOptions(16, 200, -1, false)
        );
        var mapper = new DenseVectorFieldMapper.Builder("field", IndexVersion.current(), IndexMode.STANDARD, true, false, List.of(), false)
            .indexOptions(testIndexOptions)
            .dimensions(128)
            .elementType(ElementType.FLOAT)
            .build(MapperBuilderContext.root(false, false));
        final IndexSettings enabled = IndexSettingsModule.newIndexSettings(
            "foo",
            new DenseVectorTestSettingsBuilder().intraMergeParallelism(true).build()
        );
        final IndexSettings disabled = IndexSettingsModule.newIndexSettings(
            "foo",
            new DenseVectorTestSettingsBuilder().intraMergeParallelism(false).build()
        );
        // enabled with null tp
        mapper.getKnnVectorsFormatForField(new ES93HnswVectorsFormat(), enabled, null);
        assertEquals(1, testIndexOptions.passedNumMergeWorkers);
        assertNull(testIndexOptions.passedMergingExecutorService);
        // disabled with null tp
        mapper.getKnnVectorsFormatForField(new ES93HnswVectorsFormat(), disabled, null);
        assertEquals(1, testIndexOptions.passedNumMergeWorkers);
        assertNull(testIndexOptions.passedMergingExecutorService);
        // tiny tp, means we don't have extra threads for merging
        try (var tp = new TestThreadPool(getTestName(), Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), 1).build())) {
            mapper.getKnnVectorsFormatForField(new ES93HnswVectorsFormat(), enabled, tp);
            assertEquals(1, testIndexOptions.passedNumMergeWorkers);
            assertNull(testIndexOptions.passedMergingExecutorService);

            mapper.getKnnVectorsFormatForField(new ES93HnswVectorsFormat(), disabled, tp);
            assertEquals(1, testIndexOptions.passedNumMergeWorkers);
            assertNull(testIndexOptions.passedMergingExecutorService);
        }
        // big tp
        try (var tp = new TestThreadPool(getTestName(), Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), 10).build())) {
            mapper.getKnnVectorsFormatForField(new ES93HnswVectorsFormat(), enabled, tp);
            assertNotNull(testIndexOptions.passedMergingExecutorService);
            assertEquals(10, testIndexOptions.passedNumMergeWorkers);
        }
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new DenseVectorSyntheticSourceSupport();
    }

    @Override
    protected boolean supportsEmptyInputArray() {
        return false;
    }

    private static class DenseVectorSyntheticSourceSupport implements SyntheticSourceSupport {
        private final int vectorLength = between(5, 1000);
        private final ElementType elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT, ElementType.BFLOAT16, ElementType.BIT);
        private final boolean indexed = randomBoolean();
        private final boolean indexOptionsSet = indexed && randomBoolean();

        @Override
        public SyntheticSourceExample example(int maxValues) throws IOException {
            Object value = switch (elementType) {
                case BYTE, BIT -> randomList(vectorLength, vectorLength, ESTestCase::randomByte);
                case FLOAT -> randomList(vectorLength, vectorLength, ESTestCase::randomFloat);
                case BFLOAT16 -> randomList(vectorLength, vectorLength, () -> BFloat16.truncateToBFloat16(randomFloat()));
            };
            return new SyntheticSourceExample(value, value, this::mapping);
        }

        private void mapping(XContentBuilder b) throws IOException {
            var mapping = new DenseVectorMappingBuilder().dims(elementType.dims(vectorLength)).index(indexed);
            if (indexed) {
                mapping.similarity(VectorSimilarity.L2_NORM);
            }
            if (elementType != ElementType.FLOAT || randomBoolean()) {
                mapping.elementType(elementType);
            }
            if (indexOptionsSet) {
                mapping.indexOptions(Map.of("type", "hnsw", "m", 5, "ef_construction", 50));
            }
            mapping.build(b);
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() {
            return List.of();
        }
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // The mapper expects to parse an array of values by default, it's not compatible with array of arrays.
    }

    /** {@code on_disk_merge} can be flipped by a mapping update: the index type stays the same, so the update is not rejected. */
    public void testOnDiskMergeIndexOptions() throws IOException {
        for (String type : new String[] {
            "hnsw",
            "int8_hnsw",
            "int4_hnsw",
            "flat",
            "int8_flat",
            "int4_flat",
            "bbq_hnsw",
            "bbq_flat",
            "bbq_disk" }) {
            MapperService mapperService = createMapperService(fieldMapping(b -> onDiskMergeMapping(b, type, null)));
            assertFalse(type + " defaults to off", onDiskMergeOf(mapperService));
            assertFormatCarriesOnDiskMerge(type, mapperService, false);

            merge(mapperService, fieldMapping(b -> onDiskMergeMapping(b, type, true)));
            assertTrue(type, onDiskMergeOf(mapperService));
            assertThat(type, mapperService.documentMapper().mappingSource().toString(), containsString("\"on_disk_merge\":true"));
            assertFormatCarriesOnDiskMerge(type, mapperService, true);

            merge(mapperService, fieldMapping(b -> onDiskMergeMapping(b, type, false)));
            assertFalse(type, onDiskMergeOf(mapperService));
            assertThat(type, mapperService.documentMapper().mappingSource().toString(), not(containsString("on_disk_merge")));
        }
    }

    /** @param onDiskMerge the value of {@code on_disk_merge}, or {@code null} to leave it out */
    private static void onDiskMergeMapping(XContentBuilder b, String type, Boolean onDiskMerge) throws IOException {
        Map<String, Object> indexOptions = new HashMap<>();
        indexOptions.put("type", type);
        if (onDiskMerge != null) {
            indexOptions.put("on_disk_merge", onDiskMerge);
        }
        new DenseVectorMappingBuilder().dims(64).index(true).indexOptions(indexOptions).build(b);
    }

    /**
     * {@code bbq_disk} is skipped: its format is built by its plugin, the server class throws a license error here,
     * and the plugin's {@code DirectIOIT} covers that hand-off.
     */
    private static void assertFormatCarriesOnDiskMerge(String type, MapperService mapperService, boolean onDiskMerge) {
        if (type.equals("bbq_disk")) {
            return;
        }
        Codec codec = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null).codec("default");
        KnnVectorsFormat format = ((PerFieldKnnVectorsFormat) codec.knnVectorsFormat()).getKnnVectorsFormatForField("field");
        assertThat(type, format, hasToString(containsString("onDiskMerge=" + onDiskMerge)));
    }

    private static boolean onDiskMergeOf(MapperService mapperService) {
        return getIndexOptions(mapperService, "field", DenseVectorFieldMapper.DenseVectorIndexOptions.class).isOnDiskMerge();
    }

    private static class TestDenseVectorIndexOptions extends DenseVectorFieldMapper.DenseVectorIndexOptions {

        private final DenseVectorFieldMapper.DenseVectorIndexOptions inner;
        private ExecutorService passedMergingExecutorService;
        private int passedNumMergeWorkers = -1;

        TestDenseVectorIndexOptions(DenseVectorFieldMapper.DenseVectorIndexOptions inner) {
            super(inner.type, inner.isOnDiskMerge());
            this.inner = inner;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(
            ElementType elementType,
            ExecutorService mergingExecutorService,
            int numMergeWorkers,
            ExecutorService quantizerExecutorService
        ) {
            this.passedMergingExecutorService = mergingExecutorService;
            this.passedNumMergeWorkers = numMergeWorkers;
            return inner.getVectorsFormat(elementType, mergingExecutorService, numMergeWorkers, quantizerExecutorService);
        }

        @Override
        public boolean updatableTo(DenseVectorFieldMapper.DenseVectorIndexOptions update) {
            return inner.updatableTo(update);
        }

        @Override
        boolean doEquals(DenseVectorFieldMapper.DenseVectorIndexOptions other) {
            return inner.equals(other);
        }

        @Override
        int doHashCode() {
            return inner.hashCode();
        }

        @Override
        public boolean isFlat() {
            return inner.isFlat();
        }

        @Override
        void doXContentFragment(XContentBuilder builder, Params params) throws IOException {
            inner.doXContentFragment(builder, params);
        }
    }
}
