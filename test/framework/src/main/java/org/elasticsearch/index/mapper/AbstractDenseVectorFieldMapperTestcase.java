/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractDenseVectorFieldMapperTestcase extends MapperTestCase {

    protected static final IndexVersion INDEXED_BY_DEFAULT_PREVIOUS_INDEX_VERSION = IndexVersions.V_8_10_0;
    protected final ElementType elementType;
    protected final boolean indexed;
    protected final boolean indexOptionsSet;
    protected final int dims;

    protected AbstractDenseVectorFieldMapperTestcase() {
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

    protected void indexMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
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
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_flat")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_flat\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
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
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_flat")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_flat\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
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
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            )
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
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
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            )
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int8_hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
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
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_flat")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_flat\""))
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
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
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            )
        );
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "int4_hnsw")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
        );
        // update for bbq_flat
        checker.registerUpdateCheck(
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_flat")
                .endObject(),
            b -> b.field("type", "dense_vector")
                .field("dims", dims * 16)
                .field("index", true)
                .startObject("index_options")
                .field("type", "bbq_hnsw")
                .endObject(),
            m -> assertTrue(m.toString().contains("\"type\":\"bbq_hnsw\""))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
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
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
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
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
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
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_flat")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            )
        );
        // update for bbq_hnsw
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
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
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
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
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
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
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .endObject()
            )
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "bbq_hnsw")
                    .endObject()
            ),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", dims * 16)
                    .field("index", true)
                    .startObject("index_options")
                    .field("type", "int4_hnsw")
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
