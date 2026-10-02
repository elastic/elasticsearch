/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class StatelessDirectoryFactoryTests extends ESTestCase {

    /** The file extensions that hold vector data, i.e. the ones a vector scorer reads through. */
    private static final Set<LuceneFilesExtensions> VECTOR_DATA_EXTENSIONS = EnumSet.of(
        LuceneFilesExtensions.VEC,
        LuceneFilesExtensions.VEQ,
        LuceneFilesExtensions.VEB
    );

    public void testSearchDirectoryReadsBackWrittenFile() throws IOException {
        try (Directory directory = StatelessDirectoryFactory.newSearchDirectory(createTempDir().toAbsolutePath())) {
            // It's important to close the IndexOutput so the necessary metadata gets updated
            try (var output = directory.createOutput("vectors", IOContext.DEFAULT)) {
                output.writeInt(12);
            }

            var input = directory.openInput("vectors", IOContext.DEFAULT);
            var value = input.readInt();
            assertThat(value, equalTo(12));
            input.close();
        }
    }

    /**
     * We have code (e.g. {@code KnnIndexer}) that reaches {@link StatelessDirectoryFactory} reflectively.
     * Renaming methods, changing their parameters, or making them non-static breaks that caller at runtime.
     * This test ensures the API stays stable for these consumers.
     */
    public void testReflectiveApiUse() throws Exception {
        var factoryClass = Class.forName("org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory");

        var newSearchDirectory = factoryClass.getMethod("newSearchDirectory", Path.class, Path.class, Settings.class);
        assertThat(newSearchDirectory.getReturnType(), equalTo(Directory.class));
        assertTrue("invoked with a null receiver", Modifier.isStatic(newSearchDirectory.getModifiers()));

        var newIndexDirectory = factoryClass.getMethod("newIndexDirectory", Path.class, Path.class);
        assertThat(newIndexDirectory.getReturnType(), equalTo(Directory.class));
        assertTrue("invoked with a null receiver", Modifier.isStatic(newIndexDirectory.getModifiers()));

        var logCacheStats = factoryClass.getMethod("logCacheStats", Directory.class, String.class);
        assertTrue("invoked with a null receiver", Modifier.isStatic(logCacheStats.getModifiers()));
    }

    /**
     * {@link IndexDirectory} only tracks files it created itself, so anything already on disk would be invisible to Lucene yet
     * still collide when it writes a file of the same name. The factory wipes the index path for this reason.
     * This test proves that a directory opened over an existing index starts from the empty bootstrap commit.
     */
    public void testIndexDirectoryWipesExistingIndex() throws IOException {
        Path indexPath = createTempDir();
        try (Directory directory = new MMapDirectory(indexPath); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        try (Directory directory = new MMapDirectory(indexPath)) {
            assertThat(directory.listAll().length, greaterThan(1));
        }

        try (Directory directory = StatelessDirectoryFactory.newIndexDirectory(indexPath, createTempDir())) {
            assertThat(directory.listAll(), arrayContaining(EmptyDirectory.INSTANCE.getSegmentsFileName()));
        }
    }

    /**
     * A Lucene merge reopens the flat vector file it has just written through the directory it is writing to, so on an indexing
     * node those reads go through a {@code ReopeningIndexInput}.
     */
    public void testForceMergeReadsVectorsThroughReopeningIndexInput() throws IOException {
        final int dims = 64;
        final int docsPerSegment = 200;
        final int segments = 3;

        Path indexPath = createTempDir();
        var vectorFilesRead = new LinkedHashSet<String>();
        var vectorFilesReadWithoutReopening = new LinkedHashSet<String>();

        try (Directory statelessDirectory = StatelessDirectoryFactory.newIndexDirectory(indexPath, createTempDir())) {
            var recordingDirectory = new FilterDirectory(statelessDirectory) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    var input = super.openInput(name, context);
                    if (VECTOR_DATA_EXTENSIONS.contains(LuceneFilesExtensions.fromFile(name))) {
                        vectorFilesRead.add(name);
                        if (FilterIndexInput.unwrap(input) instanceof IndexDirectory.ReopeningIndexInput == false) {
                            vectorFilesReadWithoutReopening.add(name);
                        }
                    }
                    return input;
                }
            };

            var config = new IndexWriterConfig().setCodec(
                TestUtil.alwaysKnnVectorsFormat(
                    new ES94HnswScalarQuantizedVectorsFormat(16, 100, DenseVectorFieldMapper.ElementType.FLOAT, 4, false)
                )
            ).setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(recordingDirectory, config)) {
                for (int segment = 0; segment < segments; segment++) {
                    for (int doc = 0; doc < docsPerSegment; doc++) {
                        var document = new Document();
                        document.add(new KnnFloatVectorField("vector", randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT));
                        writer.addDocument(document);
                    }
                    writer.commit();
                }
                writer.forceMerge(1);
            }

            assertThat(
                "the merge never reopened a vector file, so this no longer covers the read path it is meant to",
                vectorFilesRead,
                not(empty())
            );
            assertThat(
                "vector files were read through something other than a ReopeningIndexInput",
                vectorFilesReadWithoutReopening,
                empty()
            );

            // the merged index is readable and returns the vectors that were indexed
            try (DirectoryReader reader = DirectoryReader.open(recordingDirectory)) {
                assertThat(reader.leaves(), hasSize(1));
                assertThat(reader.numDocs(), equalTo(docsPerSegment * segments));
                var hits = new IndexSearcher(reader).search(new KnnFloatVectorQuery("vector", randomVector(dims), 10), 10);
                assertThat(hits.scoreDocs.length, equalTo(10));
            }
        }
    }

    private static float[] randomVector(int dims) {
        var vector = new float[dims];
        double norm = 0.0;
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloatBetween(-1.0f, 1.0f, true);
            norm += (double) vector[i] * vector[i];
        }
        // DOT_PRODUCT requires unit-length vectors
        float length = (float) Math.sqrt(norm);
        for (int i = 0; i < dims; i++) {
            vector[i] /= length;
        }
        return vector;
    }
}
