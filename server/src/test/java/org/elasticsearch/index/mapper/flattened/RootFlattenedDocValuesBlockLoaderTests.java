/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class RootFlattenedDocValuesBlockLoaderTests extends ESTestCase {

    private static final String FIELD = "field";
    private static final String KEYED_FIELD = FIELD + FlattenedFieldMapper.KEYED_FIELD_SUFFIX;

    public void testCanReuseWhenSegmentHasNoDocValues() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            // Documents with no value at all for the flattened field, so this segment never
            // writes any doc values for it.
            writer.addDocument(new Document());
            writer.addDocument(new Document());
            writer.forceMerge(1);

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                var blockLoader = new RootFlattenedDocValuesBlockLoader(
                    FIELD,
                    new Mapper.IgnoreAbove(null, IndexMode.STANDARD),
                    false,
                    false,
                    List.of(),
                    false,
                    FlattenedFieldMapper.PreserveLeafArrays.LOSSY
                );
                BlockLoader.ColumnAtATimeReader columnReader = blockLoader.reader(new NoopCircuitBreaker("test"), leaf);

                // Must not throw even though this segment has no doc values for the field.
                assertTrue("reader with no doc values should always be reusable", columnReader.canReuse(0));
                assertTrue("reader with no doc values should always be reusable", columnReader.canReuse(1));

                columnReader.close();
            }
        }
    }

    private static IndexReader openReader(RandomIndexWriter writer) throws IOException {
        writer.forceMerge(1);
        return ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer.w), new ShardId("test", "_na_", 0));
    }
}
