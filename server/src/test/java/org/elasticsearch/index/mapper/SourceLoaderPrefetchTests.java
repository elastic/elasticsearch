/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.codec.DocValuesBatchPrefetcher;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Loading synthetic source for documents announced across segments, a prefetched window at a time, gives the same source as
 * loading them without prefetching.
 */
public class SourceLoaderPrefetchTests extends MapperServiceTestCase {

    public void testAnnouncedDocumentsLoadTheSameSource() throws IOException {
        MapperService mapperService = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("keyword").field("type", "keyword").endObject();
            b.startObject("long").field("type", "long").endObject();
            b.startObject("double").field("type", "double").endObject();
            b.startObject("ip").field("type", "ip").endObject();
            b.startObject("flag").field("type", "boolean").endObject();
        }), randomBoolean());
        DocumentMapper mapper = mapperService.documentMapper();
        int numDocs = randomIntBetween(50, 400);
        withLuceneIndex(mapperService, writer -> {
            for (int i = 0; i < numDocs; i++) {
                writer.addDocuments(mapper.parse(source(b -> {
                    if (randomBoolean()) {
                        b.field(
                            "keyword",
                            randomBoolean() ? randomAlphaOfLength(5) : List.of(randomAlphaOfLength(3), randomAlphaOfLength(4))
                        );
                    }
                    if (randomBoolean()) {
                        b.field("long", randomLong());
                    }
                    if (randomBoolean()) {
                        b.field("double", randomDouble());
                    }
                    if (randomBoolean()) {
                        b.field("ip", "10.0.0." + randomIntBetween(0, 255));
                    }
                    if (randomBoolean()) {
                        b.field("flag", randomBoolean());
                    }
                })).docs());
                if (rarely()) {
                    writer.flush();
                }
            }
        }, reader -> {
            List<LeafReaderContext> leaves = new ArrayList<>();
            List<int[]> docs = new ArrayList<>();
            for (LeafReaderContext leaf : reader.leaves()) {
                int[] inLeaf = IntStream.range(0, leaf.reader().maxDoc()).filter(d -> randomBoolean()).toArray();
                if (inLeaf.length > 0) {
                    leaves.add(leaf);
                    docs.add(inLeaf);
                }
            }
            List<String> expected = load(mapper, reader, leaves, docs, false);
            List<String> actual = load(mapper, reader, leaves, docs, true);
            assertThat(actual, equalTo(expected));
        });
    }

    private static List<String> load(
        DocumentMapper mapper,
        DirectoryReader reader,
        List<LeafReaderContext> leaves,
        List<int[]> docs,
        boolean announce
    ) throws IOException {
        SourceLoader loader = new SourceLoader.Synthetic(
            null,
            () -> mapper.mapping().syntheticFieldLoader(null),
            SourceFieldMetrics.NOOP,
            mapper.mapping().ignoredSourceFormat()
        );
        if (announce) {
            loader.announce(new DocValuesBatchPrefetcher(randomIntBetween(1, 20)), leaves, docs);
        }
        List<String> sources = new ArrayList<>();
        for (int i = 0; i < leaves.size(); i++) {
            LeafReaderContext leaf = leaves.get(i);
            int[] inLeaf = docs.get(i);
            SourceLoader.Leaf sourceLeaf = loader.leaf(leaf, inLeaf);
            LeafStoredFieldLoader storedFields = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, inLeaf);
            for (int doc : inLeaf) {
                storedFields.advanceTo(doc);
                try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                    sourceLeaf.write(storedFields, doc, b);
                    sources.add(BytesReference.bytes(b).utf8ToString());
                }
            }
        }
        return sources;
    }
}
