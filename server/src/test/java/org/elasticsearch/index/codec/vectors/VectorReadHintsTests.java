/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import org.elasticsearch.core.CheckedFunction;
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
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/** The hints each vectors file is opened with for search, see {@link VectorReadHints}. */
public class VectorReadHintsTests extends MapperServiceTestCase {

    private static final int DIMS = 64;

    private final Case each;

    public VectorReadHintsTests(Case each) {
        this.each = each;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        return cases().stream().map(c -> new Object[] { c }).toList();
    }

    public void testRawVectorsSayHowTheyAreRead() throws IOException {
        List<Open> opens = searchOpens(each.codec(this));
        List<Open> vectorData = opens.stream().filter(Open::isVectorData).toList();
        assertTrue("opened no raw vectors: " + opens, vectorData.stream().anyMatch(Open::isRawVectors));

        for (Open open : vectorData) {
            var hints = open.context().hints();
            if (open.isRawVectors()) {
                assertThat(
                    "only raw vectors read to rescore are not read again, " + open,
                    hints,
                    has(NoReuseHint.INSTANCE, each.rescoresFromRaw())
                );
                boolean walkedOrRescored = each.rescoresFromRaw() || each.walkedByGraph();
                assertThat("raw vectors say how they are read, " + open, hints, has(DataAccessHint.RANDOM, walkedOrRescored));
                assertThat("nothing streams them for search, " + open, hints, not(hasItem(DataAccessHint.SEQUENTIAL)));
            } else {
                assertThat("quantized vectors and graphs are read again, " + open, hints, not(hasItem(NoReuseHint.INSTANCE)));
            }
        }
    }

    /** Matches hints that hold {@code hint} if {@code present}, and hints that don't otherwise. */
    public static Matcher<Iterable<? super IOContext.FileOpenHint>> has(IOContext.FileOpenHint hint, boolean present) {
        return present ? hasItem(hint) : not(hasItem(hint));
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

    /** A vectors format, whether a graph walks its raw vectors, and whether it keeps them only to rescore. */
    public record Case(
        String name,
        CheckedFunction<MapperServiceTestCase, Codec, IOException> codecFor,
        boolean walkedByGraph,
        boolean rescoresFromRaw
    ) {
        /** The codec; an index type's comes from a mapping, so it needs the test's mapper service. */
        public Codec codec(MapperServiceTestCase test) throws IOException {
            return codecFor.apply(test);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Every index type over float and bfloat16, plus disk BBQ and the formats older segments are read with. */
    public static List<Case> cases() {
        List<Case> cases = new ArrayList<>();
        for (VectorIndexType type : VectorIndexType.values()) {
            if (type == VectorIndexType.BBQ_DISK) {
                continue;
            }
            for (ElementType elementType : List.of(ElementType.FLOAT, ElementType.BFLOAT16)) {
                if (type.supportsElementType(elementType)) {
                    boolean graph = type == VectorIndexType.HNSW || type.getName().endsWith("_hnsw");
                    cases.add(
                        new Case(
                            type.getName() + " over " + elementType,
                            test -> codecFor(test, type, elementType),
                            graph,
                            type.isQuantized()
                        )
                    );
                }
            }
        }
        cases.add(diskCase("ES920DiskBBQVectorsFormat", new ES920DiskBBQVectorsFormat(384, 16)));
        cases.add(diskCase("ES940DiskBBQVectorsFormat", new ES940DiskBBQVectorsFormat(384, 16)));
        cases.add(diskCase("ES950DiskBBQVectorsFormat", new ES950DiskBBQVectorsFormat()));
        cases.add(diskCase("ESNextDiskBBQVectorsFormat", new ESNextDiskBBQVectorsFormat(384, 16, null)));
        cases.add(diskCase("ESNextDiskASHVectorsFormat", new ESNextDiskASHVectorsFormat()));
        cases.add(
            new Case(
                "ES814HnswScalarQuantizedVectorsFormat",
                test -> new OneFormatCodec(new ES814HnswScalarQuantizedRWVectorsFormat()),
                true,
                true
            )
        );
        cases.add(
            new Case(
                "ES816BinaryQuantizedVectorsFormat",
                test -> new OneFormatCodec(new ES816BinaryQuantizedRWVectorsFormat()),
                false,
                true
            )
        );
        cases.add(new Case("ES813Int8FlatVectorFormat", test -> new OneFormatCodec(new ES813Int8FlatRWVectorFormat()), false, true));
        cases.add(
            new Case("ES93ScalarQuantizedVectorsFormat", test -> new OneFormatCodec(new ES93ScalarQuantizedVectorsFormat()), false, true)
        );
        cases.add(
            new Case(
                "ES93HnswScalarQuantizedVectorsFormat",
                test -> new OneFormatCodec(new ES93HnswScalarQuantizedVectorsFormat()),
                true,
                true
            )
        );
        return cases;
    }

    private static Case diskCase(String name, KnnVectorsFormat format) {
        return new Case(name, test -> new OneFormatCodec(format), false, true);
    }

    /** Every file a search opens on a segment written with {@code codec}, with its context. */
    public static List<Open> searchOpens(Codec codec) throws IOException {
        List<Open> opens = new CopyOnWriteArrayList<>();
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
            opens.clear();
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves(), hasSize(1));
            }
            return List.copyOf(opens);
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
        return VectorTestUtils.randomFloatVector(DIMS);
    }

    /** One vectors format for every field, under the default codec's name so the segment can be read back. */
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
            opens.add(new Open(name, context));
            return super.openInput(name, context);
        }
    }
}
