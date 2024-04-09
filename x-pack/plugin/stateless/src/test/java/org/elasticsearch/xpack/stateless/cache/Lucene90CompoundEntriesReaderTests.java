/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

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

            var actualSegmentEntries = Set.of(si.getCodec().compoundFormat().getCompoundReader(directory, si, IOContext.DEFAULT).listAll());
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
