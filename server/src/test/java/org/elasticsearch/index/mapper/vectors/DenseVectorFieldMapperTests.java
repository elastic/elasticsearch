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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.vectors.IVFVectorsFormat;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.index.codec.vectors.IVFVectorsFormat.DYNAMIC_NPROBE;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.IVF_FORMAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFieldMapperTests extends MapperTestCase {

    private static final IndexVersion INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION = IndexVersions.V_8_10_0;
    private final ElementType elementType;
    private final boolean indexed;
    private final boolean indexOptionsSet;
    private final int dims;

    public DenseVectorFieldMapperTests() {
        this.elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT, ElementType.BIT);
        this.indexed = randomBoolean();
        this.indexOptionsSet = this.indexed && randomBoolean();
        this.dims = ElementType.BIT == elementType ? 4 * Byte.SIZE : 4;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        indexMapping(b, IndexVersion.current());
    }

    @Override
    protected void minimalMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        indexMapping(b, indexVersion);
    }

    private void indexMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        b.field("type", "dense_vector").field("dims", dims);
        if (elementType != ElementType.FLOAT) {
            b.field("element_type", elementType.toString());
        }
        if (indexVersion.onOrAfter(DenseVectorFieldMapper.INDEXED_BY_DEFAULT_INDEX_VERSION) || indexed) {
            // Serialize if it's new index version, or it was not the default for previous indices
            b.field("index", indexed);
        }
        if (indexVersion.onOrAfter(DenseVectorFieldMapper.DEFAULT_TO_INT8)
            && indexed
            && elementType.equals(ElementType.FLOAT)
            && indexOptionsSet == false) {
            b.startObject("index_options");
            b.field("type", "int8_hnsw");
            b.field("m", 16);
            b.field("ef_construction", 100);
            b.endObject();
        }
        if (indexed) {
            b.field("similarity", elementType == ElementType.BIT ? "l2_norm" : "dot_product");
            if (indexOptionsSet) {
                b.startObject("index_options");
                b.field("type", "hnsw");
                b.field("m", 5);
                b.field("ef_construction", 50);
                b.endObject();
            }
        }
    }

    @Override
    protected Object getSampleValueForDocument() {
        return elementType == ElementType.FLOAT ? List.of(0.5, 0.5, 0.5, 0.5) : List.of((byte) 1, (byte) 1, (byte) 1, (byte) 1);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "dims",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims)),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims + 8))
        );
        checker.registerConflictCheck(
            "similarity",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", "dot_product")),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", "l2_norm"))
        );
        checker.registerConflictCheck(
            "index",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", "dot_product")),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", false))
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .field("similarity", "dot_product")
                    .field("element_type", "byte")
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .field("similarity", "dot_product")
                    .field("element_type", "float")
            )
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .field("element_type", "float")
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 8)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .field("element_type", "bit")
            )
        );
        checker.registerConflictCheck(
            "element_type",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .field("element_type", "byte")
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 8)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .field("element_type", "bit")
            )
        );
        // update for flat
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int8_flat\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_flat\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int8_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_hnsw\""))
        );
        // update for int8_flat
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int8_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_flat\""))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "flat")
                    .endObject()
            )
        );
        // update for hnsw
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int8_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .field("m", 100)
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"hnsw\""))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .field("m", 16)
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_flat")
                    .endObject()
            )
        );
        // update for int8_hnsw
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .field("m", 256)
                .endObject(),
            m -> assertTrue(m.toString().contains("\"m\":256"))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .field("m", 256)
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_hnsw\""))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .field("m", 16)
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_flat")
                    .endObject()
            )
        );
        // update for int4_flat
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int4_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"int8_hnsw\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"hnsw\""))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_flat")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_flat")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "flat")
                    .endObject()
            )
        );
        // update for int4_hnsw
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("m", 256)
                .field("type", "int4_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"m\":256"))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .field("confidence_interval", 0.03)
                .field("m", 4)
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .field("confidence_interval", 0.03)
                .field("m", 100)
                .endObject(),
            m -> assertTrue(m.toString().contains("\"m\":100"))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .field("m", 16)
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .field("confidence_interval", 0.3)
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .field("m", 16)
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .field("m", 32)
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .field("m", 16)
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_flat")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_flat")
                    .endObject()
            )
        );
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
        assertEquals(fieldType.isIndexed(), indexed);
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
        assumeTrue("feature flag [ivf_format] must be enabled", IVF_FORMAT.isEnabled());
        {
            DocumentMapper mapperService = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 128);
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", "bbq_ivf");
                b.endObject();
            }));

            DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) mapperService.mappers().getMapper("field");
            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = (DenseVectorFieldMapper.BBQIVFIndexOptions) denseVectorFieldMapper
                .fieldType()
                .getIndexOptions();
            assertEquals(3.0F, indexOptions.rescoreVector.oversample(), 0.0F);
            assertEquals(IVFVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER, indexOptions.clusterSize);
            assertEquals(DYNAMIC_NPROBE, indexOptions.defaultNProbe);
        }
        {
            DocumentMapper mapperService = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 128);
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", "bbq_ivf");
                b.field("cluster_size", 1000);
                b.field("default_n_probe", 10);
                b.field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 2.0f));
                b.endObject();
            }));

            DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) mapperService.mappers().getMapper("field");
            DenseVectorFieldMapper.BBQIVFIndexOptions indexOptions = (DenseVectorFieldMapper.BBQIVFIndexOptions) denseVectorFieldMapper
                .fieldType()
                .getIndexOptions();
            assertEquals(2F, indexOptions.rescoreVector.oversample(), 0.0F);
            assertEquals(1000, indexOptions.clusterSize);
            assertEquals(10, indexOptions.defaultNProbe);
        }
    }

    public void testIVFParsingFailureInRelease() {
        assumeFalse("feature flag [ivf_format] must be disabled", IVF_FORMAT.isEnabled());

        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", dims)
                        .startObject("index_options")
                        .field("type", "bbq_ivf")
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("Unknown vector index options"));
    }

    public void testRescoreVectorForNonQuantized() {
        for (String indexType : List.of("hnsw", "flat")) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 1.5f))
                            .endObject()
                    )
                )
            );
            e.getMessage().contains("Mapping definition for [field] has unsupported parameters:");
        }
    }

    public void testRescoreVectorOldIndexVersion() {
        IndexVersion incompatibleVersion = randomFrom(
            IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersionUtils.getLowestReadCompatibleVersion(),
                IndexVersionUtils.getPreviousVersion(IndexVersions.ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS_BACKPORT_8_X)
            ),
            IndexVersionUtils.randomVersionBetween(
                random(),
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
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 1.5f))
                            .endObject()
                    )
                )
            );
        }
    }

    public void testRescoreZeroVectorOldIndexVersion() {
        IndexVersion incompatibleVersion = randomFrom(
            IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersionUtils.getLowestReadCompatibleVersion(),
                IndexVersionUtils.getPreviousVersion(IndexVersions.RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS_BACKPORT_8_X)
            ),
            IndexVersionUtils.randomVersionBetween(
                random(),
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
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 0f))
                            .endObject()
                    )
                )
            );
        }
    }

    public void testInvalidRescoreVector() {
        for (String indexType : List.of("int8_hnsw", "int8_flat", "int4_hnsw", "int4_flat", "bbq_hnsw", "bbq_flat")) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("foo", 1.5f))
                            .endObject()
                    )
                )
            );
            e.getMessage().contains("Invalid rescore_vector value. Missing required field oversample");
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", "foo"))
                            .endObject()
                    )
                )
            );
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 0.1f))
                            .endObject()
                    )
                )
            );
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of())
                            .endObject()
                    )
                )
            );
            e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(
                        b -> b.field("type", "dense_vector")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", indexType)
                            .field(DenseVectorFieldMapper.RescoreVector.NAME, Map.of("oversample", 10.1f))
                            .endObject()
                    )
                )
            );
        }
    }

    public void testDefaultOversampleValue() throws IOException {
        {
            DocumentMapper mapperService = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 128);
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", "bbq_hnsw");
                b.endObject();
            }));

            DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) mapperService.mappers().getMapper("field");
            DenseVectorFieldMapper.BBQHnswIndexOptions indexOptions = (DenseVectorFieldMapper.BBQHnswIndexOptions) denseVectorFieldMapper
                .fieldType()
                .getIndexOptions();
            assertEquals(3.0F, indexOptions.rescoreVector.oversample(), 0.0F);
        }
        {
            DocumentMapper mapperService = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 128);
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", "bbq_flat");
                b.endObject();
            }));

            DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) mapperService.mappers().getMapper("field");
            DenseVectorFieldMapper.BBQFlatIndexOptions indexOptions = (DenseVectorFieldMapper.BBQFlatIndexOptions) denseVectorFieldMapper
                .fieldType()
                .getIndexOptions();
            assertEquals(3.0F, indexOptions.rescoreVector.oversample(), 0.0F);
        }
        {
            DocumentMapper mapperService = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 128);
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", "int8_hnsw");
                b.endObject();
            }));

            DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) mapperService.mappers().getMapper("field");
            DenseVectorFieldMapper.Int8HnswIndexOptions indexOptions = (DenseVectorFieldMapper.Int8HnswIndexOptions) denseVectorFieldMapper
                .fieldType()
                .getIndexOptions();
            assertNull(indexOptions.rescoreVector);
        }
    }

    public void testDims() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 0);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [0]")
            );
        }
        // test max limit for non-indexed vectors
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 5000);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [5000]")
            );
        }
        // test max limit for indexed vectors
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("index", "true");
                b.field("dims", 5000);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: " + "The number of dimensions should be in the range [1, 4096] but was [5000]")
            );
        }
    }

    public void testMergeDims() throws IOException {
        XContentBuilder mapping = mapping(b -> {
            b.startObject("field");
            b.field("type", "dense_vector");
            b.endObject();
        });
        MapperService mapperService = createMapperService(mapping);

        mapping = mapping(b -> {
            b.startObject("field");
            b.field("type", "dense_vector")
                .field("dims", dims)
                .field("similarity", "cosine")
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .field("m", 16)
                .field("ef_construction", 100)
                .endObject();
            b.endObject();
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public void testLargeDimsBit() throws IOException {
        createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 1024 * Byte.SIZE);
            b.field("element_type", ElementType.BIT.toString());
        }));
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3)));

        testIndexedVector(VectorSimilarity.COSINE, mapper);
    }

    public void testIndexedVector() throws Exception {
        VectorSimilarity similarity = RandomPicks.randomFrom(random(), VectorSimilarity.values());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("similarity", similarity))
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
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("index", false))
        );

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

    public void testIndexedByteVector() throws Exception {
        VectorSimilarity similarity = RandomPicks.randomFrom(random(), VectorSimilarity.values());
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", similarity)
                    .field("element_type", "byte")
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
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", VectorSimilarity.DOT_PRODUCT)
            )
        );
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

        DocumentMapper mapperWithLargerDim = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector").field("dims", 6).field("index", true).field("similarity", VectorSimilarity.DOT_PRODUCT)
            )
        );
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
            fieldMapping(
                b -> b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", VectorSimilarity.COSINE)
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
            containsString(
                "The [cosine] similarity does not support vectors with zero magnitude. Preview of invalid vector: [-0.0, 0.0, 0.0]"
            )
        );
    }

    public void testCosineWithZeroByteVector() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", VectorSimilarity.COSINE)
                    .field("element_type", "byte")
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
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", VectorSimilarity.MAX_INNER_PRODUCT)
            )
        );
        float[] vector = { -12.1f, 2.7f, -4 };
        // Shouldn't throw
        mapper.parse(source(b -> b.array("field", vector)));
    }

    public void testWithExtremeFloatVector() throws Exception {
        for (VectorSimilarity vs : List.of(VectorSimilarity.COSINE, VectorSimilarity.DOT_PRODUCT, VectorSimilarity.COSINE)) {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", vs))
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
                fieldMapping(b -> b.field("type", "dense_vector").field("index", false).field("dims", 3).field("similarity", "l2_norm"))
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
                    b -> b.field("type", "dense_vector")
                        .field("index", false)
                        .field("dims", 3)
                        .startObject("index_options")
                        .field("type", "hnsw")
                        .field("m", 5)
                        .field("ef_construction", 100)
                        .endObject()
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
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .field("similarity", "l2_norm")
                        .field("index", true)
                        .startObject("index_options")
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("[index_options] requires field [type] to be configured"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("element_type", "foo")))
        );
        assertThat(e.getMessage(), containsString("invalid element_type [foo]; available types are "));
        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .field("similarity", "l2_norm")
                        .field("index", true)
                        .startObject("index_options")
                        .field("type", "hnsw")
                        .startObject("foo")
                        .endObject()
                        .endObject()
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
                        b -> b.field("type", "dense_vector")
                            .field("dims", 64)
                            .field("element_type", "byte")
                            .field("similarity", "l2_norm")
                            .field("index", true)
                            .startObject("index_options")
                            .field("type", quantizationKind)
                            .endObject()
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
            () -> createDocumentMapper(INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION, fieldMapping(b -> {
                b.field("type", "dense_vector").field("dims", 3).field("index", true);
            }))
        );

        assertThat(e.getMessage(), containsString("Field [index] requires field [similarity] to be configured and not null"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION, fieldMapping(b -> {
                b.field("type", "dense_vector").field("dims", 3).field("similarity", "cosine");
            }))
        );

        assertThat(
            e.getMessage(),
            containsString("Field [similarity] can only be specified for a field of type [dense_vector] when it is indexed")
        );

        e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION, fieldMapping(b -> {
                b.field("type", "dense_vector")
                    .field("dims", 3)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .field("m", 200)
                    .field("ef_construction", 20)
                    .endObject();
            }))
        );

        assertThat(
            e.getMessage(),
            containsString("Field [index_options] can only be specified for a field of type [dense_vector] when it is indexed")
        );
    }

    public void testDefaultParamsBeforeIndexByDefault() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION, fieldMapping(b -> {
            b.field("type", "dense_vector").field("dims", 3);
        }));
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) documentMapper.mappers().getMapper("field");
        DenseVectorFieldType denseVectorFieldType = denseVectorFieldMapper.fieldType();

        assertFalse(denseVectorFieldType.isIndexed());
        assertNull(denseVectorFieldType.getSimilarity());
    }

    public void testParamsBeforeIndexByDefault() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION, fieldMapping(b -> {
            b.field("type", "dense_vector").field("dims", 3).field("index", true).field("similarity", "dot_product");
        }));
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) documentMapper.mappers().getMapper("field");
        DenseVectorFieldType denseVectorFieldType = denseVectorFieldMapper.fieldType();

        assertTrue(denseVectorFieldType.isIndexed());
        assertEquals(VectorSimilarity.DOT_PRODUCT, denseVectorFieldType.getSimilarity());
    }

    public void testDefaultParamsIndexByDefault() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(fieldMapping(b -> { b.field("type", "dense_vector").field("dims", 3); }));
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) documentMapper.mappers().getMapper("field");
        DenseVectorFieldType denseVectorFieldType = denseVectorFieldMapper.fieldType();

        assertTrue(denseVectorFieldType.isIndexed());
        assertEquals(VectorSimilarity.COSINE, denseVectorFieldType.getSimilarity());
    }

    public void testValidateOnBuild() {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        // Build a dense vector field mapper with float element type, which will trigger int8 HNSW index options
        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("test", IndexVersion.current()).elementType(ElementType.FLOAT)
            .build(context);

        // Change the element type to byte, which is incompatible with int8 HNSW index options
        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(ElementType.BYTE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.build(context));
        assertThat(e.getMessage(), containsString("[element_type] cannot be [byte] when using index type [int8_hnsw]"));
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
            XContentBuilder fieldMapping = fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", dims);
                b.field("index", index);
                if (index) {
                    b.field("similarity", "dot_product");
                }
            });

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

    public void testCosineDenseVectorValues() throws IOException {
        final int dims = randomIntBetween(64, 2048);
        VectorSimilarity similarity = VectorSimilarity.COSINE;
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", similarity))
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
            IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersions.NEW_SPARSE_VECTOR),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", similarity))
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
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", dims).field("index", true).field("similarity", similarity))
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
                b -> b.field("type", "dense_vector")
                    .field("dims", dims)
                    .field("index", true)
                    .field("similarity", similarity)
                    .field("element_type", "byte")
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
                    random(),
                    IndexVersions.V_8_0_0,
                    IndexVersionUtils.getPreviousVersion(DenseVectorFieldMapper.NORMALIZE_COSINE)
                ),
                ElementType.FLOAT
            )
        );
        assertEquals(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarity.COSINE.vectorSimilarityFunction(
                IndexVersionUtils.randomVersionBetween(random(), DenseVectorFieldMapper.NORMALIZE_COSINE, IndexVersion.current()),
                ElementType.FLOAT
            )
        );
        assertEquals(
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarity.L2_NORM.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.BYTE)
        );
        assertEquals(
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarity.L2_NORM.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.FLOAT)
        );
        assertEquals(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarity.DOT_PRODUCT.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.BYTE)
        );
        assertEquals(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarity.DOT_PRODUCT.vectorSimilarityFunction(IndexVersionUtils.randomVersion(), ElementType.FLOAT)
        );
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
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft, fdt)).thenAnswer(inv -> fieldDataLookup(mapperService).apply(ft, () -> {
            throw new UnsupportedOperationException();
        }, fdt));
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), 0);
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
        b.field("type", "dense_vector").field("dims", randomIntBetween(2, 4096)).field("element_type", "float");
        if (randomBoolean()) {
            b.field("index", true).field("similarity", randomFrom(VectorSimilarity.values()).toString());
        }
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) ft;
        return switch (vectorFieldType.getElementType()) {
            case BYTE -> randomByteArrayOfLength(vectorFieldType.getVectorDimensions());
            case FLOAT -> {
                float[] floats = new float[vectorFieldType.getVectorDimensions()];
                float magnitude = 0;
                for (int i = 0; i < floats.length; i++) {
                    float f = randomFloat();
                    floats[i] = f;
                    magnitude += f * f;
                }
                magnitude = (float) Math.sqrt(magnitude);
                if (VectorSimilarity.DOT_PRODUCT.equals(vectorFieldType.getSimilarity())) {
                    for (int i = 0; i < floats.length; i++) {
                        floats[i] /= magnitude;
                    }
                }
                yield floats;
            }
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
                b -> b.field("type", "dense_vector")
                    .field("element_type", "byte")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", VectorSimilarity.COSINE)
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
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("element_type", "byte");
            b.field("dims", 3);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.field("m", 3);
            b.field("ef_construction", 10);
            b.endObject();
        }));

        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 128, 0, 0 }),
                3,
                3,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("element_type [byte] vectors do not support NaN values but found [NaN] at dim [0];"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.POSITIVE_INFINITY, 0f, 0.0f }),
                3,
                3,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors do not support infinite values but found [-Infinity] at dim [1];")
        );
    }

    public void testFloatVectorQueryBoundaries() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("element_type", "float");
            b.field("dims", 3);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.field("m", 3);
            b.field("ef_construction", 10);
            b.endObject();
        }));

        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.NaN, 0f, 0.0f }),
                3,
                3,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("element_type [float] vectors do not support NaN values but found [NaN] at dim [0];"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> denseVectorFieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { Float.POSITIVE_INFINITY, 0f, 0.0f }),
                3,
                3,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
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
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            if (setM) {
                b.field("m", m);
            }
            if (setEfConstruction) {
                b.field("ef_construction", efConstruction);
            }
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        KnnVectorsFormat knnVectorsFormat;
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
        String expectedString = "ES910HnswReducedHeapVectorsFormat(name=ES910HnswReducedHeapVectorsFormat, maxConn="
            + (setM ? m : DEFAULT_MAX_CONN)
            + ", beamWidth="
            + (setEfConstruction ? efConstruction : DEFAULT_BEAM_WIDTH)
            + ", flatVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer())"
            + ")";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

    public void testKnnQuantizedFlatVectorsFormat() throws IOException {
        boolean setConfidenceInterval = randomBoolean();
        float confidenceInterval = (float) randomDoubleBetween(0.90f, 1.0f, true);
        for (String quantizedFlatFormat : new String[] { "int8_flat", "int4_flat" }) {
            MapperService mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", dims);
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", quantizedFlatFormat);
                if (setConfidenceInterval) {
                    b.field("confidence_interval", confidenceInterval);
                }
                b.endObject();
            }));
            CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
            Codec codec = codecService.codec("default");
            KnnVectorsFormat knnVectorsFormat;
            if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
                assertThat(codec, instanceOf(PerFieldMapperCodec.class));
                knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
            } else {
                if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                    codec = deduplicateFieldInfosCodec.delegate();
                }
                assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
                knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
            }
            VectorScorerFactory factory = VectorScorerFactory.instance().orElse(null);
            String expectedString = "ES813Int8FlatVectorFormat(name=ES813Int8FlatVectorFormat, innerFormat="
                + "ES814ScalarQuantizedVectorsFormat(name=ES814ScalarQuantizedVectorsFormat,"
                + " confidenceInterval="
                + (setConfidenceInterval ? Float.toString(confidenceInterval) : (quantizedFlatFormat.equals("int4_flat") ? "0.0" : null))
                + ", bits="
                + (quantizedFlatFormat.equals("int4_flat") ? 4 : 7)
                + ", compressed="
                + quantizedFlatFormat.equals("int4_flat")
                + ", flatVectorScorer=ESFlatVectorsScorer("
                + "delegate=ScalarQuantizedVectorScorer(nonQuantizedDelegate=DefaultFlatVectorScorer())"
                + ", factory="
                + (factory != null ? factory : "null")
                + "), "
                + "rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer())))";
            assertEquals(expectedString, knnVectorsFormat.toString());
        }
    }

    public void testKnnQuantizedHNSWVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        boolean setConfidenceInterval = randomBoolean();
        float confidenceInterval = (float) randomDoubleBetween(0.90f, 1.0f, true);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "int8_hnsw");
            b.field("m", m);
            b.field("ef_construction", efConstruction);
            if (setConfidenceInterval) {
                b.field("confidence_interval", confidenceInterval);
            }
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        KnnVectorsFormat knnVectorsFormat;
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
        VectorScorerFactory factory = VectorScorerFactory.instance().orElse(null);
        String expectedString = "ES814HnswScalarQuantizedVectorsFormat(name=ES814HnswScalarQuantizedVectorsFormat, maxConn="
            + m
            + ", beamWidth="
            + efConstruction
            + ", flatVectorFormat=ES814ScalarQuantizedVectorsFormat("
            + "name=ES814ScalarQuantizedVectorsFormat, confidenceInterval="
            + (setConfidenceInterval ? confidenceInterval : null)
            + ", bits=7, compressed=false, "
            + "flatVectorScorer=ESFlatVectorsScorer(delegate=ScalarQuantizedVectorScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()), "
            + "factory="
            + (factory != null ? factory : "null")
            + "), "
            + "rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer())"
            + "))";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

    public void testKnnBBQHNSWVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        final int dims = randomIntBetween(64, 4096);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "bbq_hnsw");
            b.field("m", m);
            b.field("ef_construction", efConstruction);
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        KnnVectorsFormat knnVectorsFormat;
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
        String expectedString = "ES818HnswBinaryQuantizedVectorsFormat(name=ES818HnswBinaryQuantizedVectorsFormat, maxConn="
            + m
            + ", beamWidth="
            + efConstruction
            + ", flatVectorFormat=ES818BinaryQuantizedVectorsFormat("
            + "name=ES818BinaryQuantizedVectorsFormat, "
            + "flatVectorScorer=ES818BinaryFlatVectorsScorer(nonQuantizedDelegate=DefaultFlatVectorScorer())))";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

    public void testKnnBBQIVFVectorsFormat() throws IOException {
        assumeTrue("feature flag [ivf_format] must be enabled", IVF_FORMAT.isEnabled());
        final int dims = randomIntBetween(64, 4096);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "bbq_ivf");
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        KnnVectorsFormat knnVectorsFormat;
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
        String expectedString = "IVFVectorsFormat(vectorPerCluster=384)";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

    public void testInvalidVectorDimensionsBBQ() {
        for (String quantizedFlatFormat : new String[] { "bbq_hnsw", "bbq_flat" }) {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", randomIntBetween(1, 63));
                b.field("element_type", "float");
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", quantizedFlatFormat);
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("does not support dimensions fewer than 64"));
        }
    }

    public void testKnnHalfByteQuantizedHNSWVectorsFormat() throws IOException {
        final int m = randomIntBetween(1, DEFAULT_MAX_CONN + 10);
        final int efConstruction = randomIntBetween(1, DEFAULT_BEAM_WIDTH + 10);
        boolean setConfidenceInterval = randomBoolean();
        float confidenceInterval = (float) randomDoubleBetween(0.90f, 1.0f, true);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "int4_hnsw");
            b.field("m", m);
            b.field("ef_construction", efConstruction);
            if (setConfidenceInterval) {
                b.field("confidence_interval", confidenceInterval);
            }
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        KnnVectorsFormat knnVectorsFormat;
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
        VectorScorerFactory factory = VectorScorerFactory.instance().orElse(null);
        String expectedString = "ES814HnswScalarQuantizedVectorsFormat(name=ES814HnswScalarQuantizedVectorsFormat, maxConn="
            + m
            + ", beamWidth="
            + efConstruction
            + ", flatVectorFormat=ES814ScalarQuantizedVectorsFormat("
            + "name=ES814ScalarQuantizedVectorsFormat, confidenceInterval="
            + (setConfidenceInterval ? confidenceInterval : 0.0f)
            + ", bits=4, compressed=true, "
            + "flatVectorScorer=ESFlatVectorsScorer(delegate=ScalarQuantizedVectorScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()), "
            + "factory="
            + (factory != null ? factory : "null")
            + "), "
            + "rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer())"
            + "))";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

    public void testInvalidVectorDimensions() {
        for (String quantizedFlatFormat : new String[] { "int4_hnsw", "int4_flat" }) {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "dense_vector");
                b.field("dims", 5);
                b.field("element_type", "float");
                b.field("index", true);
                b.field("similarity", "dot_product");
                b.startObject("index_options");
                b.field("type", quantizedFlatFormat);
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("only supports even dimensions"));
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
        private final int dims = between(5, 1000);
        private final ElementType elementType = randomFrom(ElementType.BYTE, ElementType.FLOAT, ElementType.BIT);
        private final boolean indexed = randomBoolean();
        private final boolean indexOptionsSet = indexed && randomBoolean();

        @Override
        public SyntheticSourceExample example(int maxValues) throws IOException {
            Object value = switch (elementType) {
                case BYTE, BIT:
                    yield randomList(dims, dims, ESTestCase::randomByte);
                case FLOAT:
                    yield randomList(dims, dims, ESTestCase::randomFloat);
            };
            return new SyntheticSourceExample(value, value, this::mapping);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "dense_vector");
            if (elementType == ElementType.BYTE || elementType == ElementType.BIT || randomBoolean()) {
                b.field("element_type", elementType.toString());
            }
            b.field("dims", elementType == ElementType.BIT ? dims * Byte.SIZE : dims);
            if (indexed) {
                b.field("index", true);
                b.field("similarity", "l2_norm");
                if (indexOptionsSet) {
                    b.startObject("index_options");
                    b.field("type", "hnsw");
                    b.field("m", 5);
                    b.field("ef_construction", 50);
                    b.endObject();
                }
            } else {
                b.field("index", false);
            }
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
}
