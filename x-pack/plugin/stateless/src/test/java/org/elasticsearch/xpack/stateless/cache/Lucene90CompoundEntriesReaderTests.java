/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.hamcrest.Matchers.equalTo;

public class Lucene90CompoundEntriesReaderTests extends ESTestCase {

    public void testReadEntries() throws IOException {
        var tmpDir = createTempDir();

        IndexWriterConfig conf = new IndexWriterConfig().setUseCompoundFile(true);
        try (Directory directory = FSDirectory.open(tmpDir); IndexWriter writer = new IndexWriter(directory, conf)) {
            for (int i = 0; i < randomIntBetween(50, 100); i++) {
                writer.addDocument(createDocument());
            }
        }

        try (Directory directory = FSDirectory.open(tmpDir)) {
            var infos = Lucene.readSegmentInfos(directory);
            var si = infos.info(0).info;

            var actualSegmentEntries = Set.of(si.getCodec().compoundFormat().getCompoundReader(directory, si).listAll());
            var parsedSegmentEntries = prependSegmentName(
                si.name,
                Lucene90CompoundEntriesReader.readEntries(directory.openInput(si.name + ".cfe", IOContext.DEFAULT)).keySet()
            );

            assertThat(parsedSegmentEntries, equalTo(actualSegmentEntries));
        }

    }

    private static List<IndexableField> createDocument() {
        return List.of(new TextField("id", randomIdentifier(), Field.Store.YES), new IntField("value", randomInt(), Field.Store.YES));
    }

    private static Set<String> prependSegmentName(String segmentName, Set<String> files) {
        return files.stream().map(file -> segmentName + file).collect(toUnmodifiableSet());
    }
}
