/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Level;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@LuceneTestCase.SuppressCodecs("*") // only use our own codecs
@ESTestCase.WithoutEntitlements // requires entitlement delegation ES-10920
public class DirectIOIT extends ESIntegTestCase {

    private static boolean SUPPORTED;

    @BeforeClass
    public static void checkSupported() {
        Path path = createTempDir("directIOProbe");
        try (Directory dir = open(path); IndexOutput out = dir.createOutput("out", IOContext.DEFAULT)) {
            out.writeString("test");
            SUPPORTED = true;
        } catch (IOException e) {
            SUPPORTED = false;
        }
    }

    static DirectIODirectory open(Path path) throws IOException {
        return new DirectIODirectory(FSDirectory.open(path)) {
            @Override
            protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
                return true;
            }
        };
    }

    private final String type;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.<Object[]>of(new Object[] { "bbq_disk" });
    }

    public DirectIOIT(String type) {
        this.type = type;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    private String indexVectors(boolean directIO) {
        String indexName = "test-vectors-" + directIO;
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put(InternalSettingsPlugin.USE_COMPOUND_FILE.getKey(), false))
                .setMapping(Strings.format("""
                    {
                      "properties": {
                        "fooVector": {
                          "type": "dense_vector",
                          "dims": 64,
                          "element_type": "float",
                          "index": true,
                          "similarity": "l2_norm",
                          "index_options": {
                            "type": "%s",
                            "on_disk_rescore": %s
                          }
                        }
                      }
                    }
                    """, type, directIO))
        );
        ensureGreen(indexName);

        for (int i = 0; i < 1000; i++) {
            indexDoc(indexName, Integer.toString(i), "fooVector", IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
        }
        refresh();
        assertBBQIndexType(indexName, type); // test assertion to ensure that the correct index type is being used
        return indexName;
    }

    @SuppressWarnings("unchecked")
    static void assertBBQIndexType(String indexName, String type) {
        var response = indicesAdmin().prepareGetFieldMappings(indexName).setFields("fooVector").get();
        var map = (Map<String, Object>) response.fieldMappings(indexName, "fooVector").sourceAsMap().get("fooVector");
        assertThat((String) ((Map<String, Object>) map.get("index_options")).get("type"), is(equalTo(type)));
    }

    @TestLogging(value = "org.elasticsearch.index.store.FsDirectoryFactory:DEBUG", reason = "to capture trace logging for direct IO")
    public void testDirectIOUsed() {
        try (MockLog mockLog = MockLog.capture(FsDirectoryFactory.class)) {
            // we're just looking for some evidence direct IO is used (or not)
            MockLog.LoggingExpectation expectation = SUPPORTED
                ? new MockLog.PatternSeenEventExpectation(
                    "Direct IO used",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Opening .*\\.vec with direct IO"
                )
                : new MockLog.PatternSeenEventExpectation(
                    "Direct IO not used",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Could not open .*\\.vec with direct IO"
                );
            mockLog.addExpectation(expectation);

            String indexName = indexVectors(true);

            // do a search
            var knn = List.of(new KnnSearchBuilder("fooVector", new VectorData(null, new byte[64]), 10, 20, 10f, null, null));
            assertHitCount(prepareSearch(indexName).setKnnSearch(knn), 10);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.elasticsearch.index.store.FsDirectoryFactory:DEBUG", reason = "to capture trace logging for direct IO")
    public void testDirectIONotUsed() {
        try (MockLog mockLog = MockLog.capture(FsDirectoryFactory.class)) {
            // nothing about direct IO should be logged at all
            MockLog.LoggingExpectation expectation = SUPPORTED
                ? new MockLog.PatternNotSeenEventExpectation(
                    "Direct IO used",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Opening .*\\.vec with direct IO"
                )
                : new MockLog.PatternNotSeenEventExpectation(
                    "Direct IO not used",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Could not open .*\\.vec with direct IO"
                );
            mockLog.addExpectation(expectation);

            String indexName = indexVectors(false);

            // do a search
            var knn = List.of(new KnnSearchBuilder("fooVector", new VectorData(null, new byte[64]), 10, 20, 10f, null, null));
            assertHitCount(prepareSearch(indexName).setKnnSearch(knn), 10);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false; // we require to always use the "real" hybrid directory
    }
}
