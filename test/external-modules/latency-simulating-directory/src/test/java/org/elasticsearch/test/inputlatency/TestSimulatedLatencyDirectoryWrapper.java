/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.inputlatency;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.hamcrest.Matchers.greaterThan;

public class TestSimulatedLatencyDirectoryWrapper extends ESTestCase {

    public void testBufferRunnableIsCalled() throws IOException {
        Settings build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.NIOFS.name().toLowerCase(Locale.ROOT))
            .build();
        Map<String, LongAdder> counters = new HashMap<>();
        SimulatedLatencyDirectoryWrapper wrapper = new SimulatedLatencyDirectoryWrapper(filename -> {
            counters.computeIfAbsent(filename, n -> new LongAdder()).increment();
        });
        try (
            Directory directory = wrapper.wrap(newDirectory(build), null);
            RandomIndexWriter w = new RandomIndexWriter(random(), directory)
        ) {
            // index a bunch of docs, then do some searches and check that the counters map is updated.
            Document doc = new Document();
            for (int i = 0; i < 200; i++) {
                doc.clear();
                doc.add(new TextField("text", English.intToEnglish(i), Field.Store.NO));
                doc.add(new IntField("int", i, Field.Store.NO));
                doc.add(new DoubleField("double", i * 1.5f, Field.Store.NO));
                w.addDocument(doc);
            }

            w.commit();
            w.flush();

            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            counters.clear();

            searcher.search(new TermQuery(new Term("text", "four")), 10);
            assertThat(counters.size(), greaterThan(0));

            reader.close();
        }
    }

    private Directory newDirectory(Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(idxSettings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(idxSettings.getIndex(), 0));
        return new FsDirectoryFactory().newDirectory(idxSettings, path);
    }

}
