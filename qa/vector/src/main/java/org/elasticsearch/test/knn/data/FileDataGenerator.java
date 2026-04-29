/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.test.knn.IndexVectorReader;
import org.elasticsearch.test.knn.KnnIndexTester;
import org.elasticsearch.test.knn.KnnIndexer;
import org.elasticsearch.test.knn.KnnSearcher;
import org.elasticsearch.test.knn.SearchParameters;
import org.elasticsearch.test.knn.TestConfiguration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Random;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

/**
 * A {@link DataGenerator} that reads vectors from files on disk.
 * Used for both GCP bucket datasets and custom datasets.
 */
public class FileDataGenerator implements DataGenerator {

    private final TestConfiguration config;

    FileDataGenerator(TestConfiguration config) {
        this.config = config;
    }

    @Override
    public KnnIndexTester.IndexingSetup createIndexingSetup() throws IOException {
        IndexVectorReader.MultiFileVectorReader reader = IndexVectorReader.MultiFileVectorReader.create(
            config.docVectors(),
            config.dimensions(),
            config.vectorEncoding().luceneEncoding(),
            config.numDocs()
        );
        return new KnnIndexTester.IndexingSetup(reader, new KnnIndexer.DefaultDocumentFactory(), reader.totalDocs());
    }

    @Override
    public KnnSearcher.SearchSetup createSearchSetup(KnnSearcher searcher, SearchParameters searchParameters) throws IOException {
        float[][] floatQueries = null;
        byte[][] byteQueries = null;
        int offsetByteSize = 0;

        try (FileChannel input = FileChannel.open(searcher.queryPath())) {
            long queryPathSizeInBytes = input.size();
            if (searcher.dim() == -1) {
                offsetByteSize = 4;
                ByteBuffer preamble = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                int bytesRead = Channels.readFromFileChannel(input, 0, preamble);
                if (bytesRead < 4) {
                    throw new IllegalArgumentException("queryPath \"" + searcher.queryPath() + "\" does not contain a valid dims?");
                }
                searcher.setDim(preamble.getInt(0));
                if (searcher.dim() <= 0) {
                    throw new IllegalArgumentException(
                        "queryPath \"" + searcher.queryPath() + "\" has invalid dimension: " + searcher.dim()
                    );
                }
            }
            if (queryPathSizeInBytes % (((long) searcher.dim() * searcher.vectorEncoding().byteSize + offsetByteSize)) != 0) {
                throw new IllegalArgumentException(
                    "queryPath \"" + searcher.queryPath() + "\" does not contain a whole number of vectors?  size=" + queryPathSizeInBytes
                );
            }
            logger.info(
                "queryPath size: {} bytes, assuming vector count is {}",
                queryPathSizeInBytes,
                queryPathSizeInBytes / ((long) searcher.dim() * searcher.vectorEncoding().byteSize + offsetByteSize)
            );
            IndexVectorReader.VectorReader targetReader = IndexVectorReader.VectorReader.create(
                input,
                searcher.dim(),
                searcher.vectorEncoding(),
                offsetByteSize
            );
            switch (searcher.vectorEncoding()) {
                case BYTE -> {
                    byteQueries = new byte[searcher.numQueryVectors()][searcher.dim()];
                    for (int i = 0; i < searcher.numQueryVectors(); i++) {
                        targetReader.next(byteQueries[i]);
                    }
                }
                case FLOAT32 -> {
                    floatQueries = new float[searcher.numQueryVectors()][searcher.dim()];
                    for (int i = 0; i < searcher.numQueryVectors(); i++) {
                        targetReader.next(floatQueries[i]);
                        if (config.normalizeVectors()) {
                            VectorUtil.l2normalize(floatQueries[i]);
                        }
                    }
                }
            }
        }

        Query selectivityFilter = searchParameters.filterSelectivity() < 1f
            ? KnnSearcher.generateRandomQuery(
                new Random(searchParameters.seed()),
                searcher.indexPath(),
                searcher.numDocs(),
                searchParameters.filterSelectivity(),
                searchParameters.filterCached()
            )
            : null;

        var provider = new KnnSearcher.SimpleFilterQueryProvider(searcher.numQueryVectors(), selectivityFilter);
        var consumer = new KnnSearcher.FileBasedResultsConsumer(searcher, offsetByteSize, selectivityFilter);
        return new KnnSearcher.SearchSetup(floatQueries, byteQueries, provider, consumer);
    }

    @Override
    public boolean hasQueries() {
        return config.queryVectors() != null;
    }

    @Override
    public Sort getIndexSort() {
        return null;
    }
}
