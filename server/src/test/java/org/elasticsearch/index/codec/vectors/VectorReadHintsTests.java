/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoReuseHint;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es816.ES816BinaryQuantizedRWVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorIndexType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The hints each vectors file is opened with for search, for every vectors format, see {@link VectorReadHints}. */
public class VectorReadHintsTests extends MapperServiceTestCase {

    private static final int DIMS = 64;

    public void testEveryIndexTypeSaysHowItsVectorsAreRead() throws IOException {
        for (Case each : cases(this)) {
            List<Open> opens = searchOpens(each.codec());
            List<Open> vectorData = opens.stream().filter(Open::isVectorData).toList();
            assertTrue(each + " opened no raw vectors: " + opens, vectorData.stream().anyMatch(Open::isRawVectors));

            for (Open open : vectorData) {
                var hints = open.context().hints();
                if (open.isRawVectors()) {
                    assertEquals(
                        each + ": only raw vectors read to rescore are not read again, " + open,
                        each.rescoresFromRaw(),
                        hints.contains(NoReuseHint.INSTANCE)
                    );
                    boolean walkedOrRescored = each.rescoresFromRaw() || each.walkedByGraph();
                    assertEquals(
                        each + ": raw vectors say how they are read, " + open,
                        walkedOrRescored,
                        hints.contains(DataAccessHint.RANDOM)
                    );
                    assertFalse(each + ": nothing streams them for search, " + open, hints.contains(DataAccessHint.SEQUENTIAL));
                } else {
                    assertFalse(each + ": quantized vectors and graphs are read again, " + open, hints.contains(NoReuseHint.INSTANCE));
                }
            }
        }
    }

    /** One open of a file, with the context it was opened with. */
    public record Open(String name, IOContext context) {
        public boolean isVectorData() {
            return context.hints().contains(FileDataHint.KNN_VECTORS) && context.hints().contains(FileTypeHint.DATA);
        }

        public boolean isRawVectors() {
            return name.endsWith(".vec");
        }
    }

    /**
     * A vectors format to check: what a mapping asks for, whether a graph walks its raw vectors, and whether it keeps them
     * only to rescore.
     */
    public record Case(String name, Codec codec, boolean walkedByGraph, boolean rescoresFromRaw) {
        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Every index type a mapping can ask for, over float and bfloat16 vectors, plus formats built directly: disk BBQ, which a
     * mapping only gets with a license, and the formats older segments are still read with.
     */
    public static List<Case> cases(MapperServiceTestCase test) throws IOException {
        List<Case> cases = new ArrayList<>();
        for (VectorIndexType type : VectorIndexType.values()) {
            if (type == VectorIndexType.BBQ_DISK) {
                continue;
            }
            for (ElementType elementType : List.of(ElementType.FLOAT, ElementType.BFLOAT16)) {
                if (type.supportsElementType(elementType)) {
                    boolean graph = type == VectorIndexType.HNSW || type.getName().endsWith("_hnsw");
                    Codec codec = codecFor(test, type, elementType);
                    cases.add(new Case(type.getName() + " over " + elementType, codec, graph, type.isQuantized()));
                }
            }
        }
        cases.add(diskCase("ES920DiskBBQVectorsFormat", new ES920DiskBBQVectorsFormat(384, 16)));
        cases.add(diskCase("ES940DiskBBQVectorsFormat", new ES940DiskBBQVectorsFormat(384, 16)));
        cases.add(diskCase("ES950DiskBBQVectorsFormat", new ES950DiskBBQVectorsFormat()));
        cases.add(diskCase("ESNextDiskBBQVectorsFormat", new ESNextDiskBBQVectorsFormat(384, 16, null)));
        cases.add(diskCase("ESNextDiskASHVectorsFormat", new ESNextDiskASHVectorsFormat()));
        cases.add(
            new Case("ES814HnswScalarQuantizedVectorsFormat", new OneFormatCodec(new ES814HnswScalarQuantizedRWVectorsFormat()), true, true)
        );
        cases.add(
            new Case("ES816BinaryQuantizedVectorsFormat", new OneFormatCodec(new ES816BinaryQuantizedRWVectorsFormat()), false, true)
        );
        cases.add(new Case("ES813Int8FlatVectorFormat", new OneFormatCodec(new ES813Int8FlatRWVectorFormat()), false, true));
        cases.add(new Case("ES93ScalarQuantizedVectorsFormat", new OneFormatCodec(new ES93ScalarQuantizedVectorsFormat()), false, true));
        cases.add(
            new Case("ES93HnswScalarQuantizedVectorsFormat", new OneFormatCodec(new ES93HnswScalarQuantizedVectorsFormat()), true, true)
        );
        return cases;
    }

    private static Case diskCase(String name, KnnVectorsFormat format) {
        return new Case(name, new OneFormatCodec(format), false, true);
    }

    /** Indexes a segment with {@code codec}, opens it for search, and returns every file the search opened, with its context. */
    public static List<Open> searchOpens(Codec codec) throws IOException {
        List<Open> opens = new ArrayList<>();
        try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 256; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("field", randomVector(), VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            synchronized (opens) {
                opens.clear();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
            }
            synchronized (opens) {
                return List.copyOf(opens);
            }
        }
    }

    /** The codec a mapping of {@code type} over {@code elementType} gets. */
    public static Codec codecFor(MapperServiceTestCase test, VectorIndexType type, ElementType elementType) throws IOException {
        MapperService mapperService = test.createMapperService(MapperServiceTestCase.fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", DIMS);
            b.field("index", true);
            b.field("similarity", "l2_norm");
            b.field("element_type", elementType.toString());
            b.startObject("index_options");
            b.field("type", type.getName());
            b.endObject();
        }));
        return new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null).codec("default");
    }

    public static float[] randomVector() {
        float[] v = new float[DIMS];
        for (int i = 0; i < DIMS; i++) {
            v[i] = randomFloat();
        }
        return v;
    }

    /** Uses one vectors format for every field, keeping the default codec's name so the segment can be read back. */
    private static class OneFormatCodec extends FilterCodec {
        private final KnnVectorsFormat format;

        OneFormatCodec(KnnVectorsFormat format) {
            super(Codec.getDefault().getName(), Codec.getDefault());
            this.format = format;
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new PerFieldKnnVectorsFormat() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return format;
                }
            };
        }
    }

    /** Records every open, in order. */
    public static class RecordingDirectory extends FilterDirectory {
        private final List<Open> opens;

        public RecordingDirectory(Directory in, List<Open> opens) {
            super(in);
            this.opens = opens;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            synchronized (opens) {
                opens.add(new Open(name, context));
            }
            return super.openInput(name, context);
        }
    }
}
