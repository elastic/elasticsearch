/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

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
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
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
        } catch (IOException | UnsupportedOperationException e) {
            SUPPORTED = false;
        }
    }

    @Before
    public void resetLicensing() {
        enableLicensing();
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
        return Stream.of("bbq_disk").map(s -> new Object[] { s }).toList();
    }

    public DirectIOIT(String type) {
        this.type = type;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class, LocalStateDiskBBQ.class);
    }

    private String indexVectors(boolean directIO) {
        return indexVectors(directIO, false);
    }

    private String indexVectors(boolean directIO, boolean onDiskMerge) {
        String indexName = "test-vectors-" + directIO + "-" + onDiskMerge;
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
                            "on_disk_rescore": %s,
                            "on_disk_merge": %s
                          }
                        }
                      }
                    }
                    """, type, directIO, onDiskMerge))
        );
        ensureGreen(indexName);

        for (int i = 0; i < 1000; i++) {
            indexDoc(indexName, Integer.toString(i), "fooVector", IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
        }
        refresh();
        assertIndexType(indexName, type); // test assertion to ensure that the correct index type is being used
        return indexName;
    }

    static void assertIndexType(String indexName, String type) {
        var response = indicesAdmin().prepareGetFieldMappings(indexName).setFields("fooVector").get();
        var map = (Map<?, ?>) response.fieldMappings(indexName, "fooVector").sourceAsMap().get("fooVector");
        assertThat(((Map<?, ?>) map.get("index_options")).get("type"), is(equalTo(type)));
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
            var knn = List.of(new KnnSearchBuilder("fooVector", VectorData.fromBytes(new byte[64]), 10, 20, 10f, null, null));
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
            var knn = List.of(new KnnSearchBuilder("fooVector", VectorData.fromBytes(new byte[64]), 10, 20, 10f, null, null));
            assertHitCount(prepareSearch(indexName).setKnnSearch(knn), 10);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.elasticsearch.index.store.FsDirectoryFactory:DEBUG", reason = "to capture trace logging for direct IO")
    public void testDirectIOUsedForMerges() {
        try (MockLog mockLog = MockLog.capture(FsDirectoryFactory.class)) {
            // the plugin builds the bbq_disk format, so this is the path a real distribution takes: the merged raw
            // vector file is created with direct IO when the field asks for it (or the attempt is logged where the
            // filesystem declines); rescoring is off
            MockLog.LoggingExpectation expectation = SUPPORTED
                ? new MockLog.PatternSeenEventExpectation(
                    "Direct IO used for the merge",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Creating .*\\.vec with direct IO"
                )
                : new MockLog.PatternSeenEventExpectation(
                    "Direct IO not used for the merge",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Could not create .*\\.vec with direct IO"
                );
            mockLog.addExpectation(expectation);
            if (SUPPORTED) {
                // the create is logged before the attempt: a silent fallback to a buffered output must not pass
                mockLog.addExpectation(
                    new MockLog.PatternNotSeenEventExpectation(
                        "No fallback from direct IO for the merge",
                        FsDirectoryFactory.class.getCanonicalName(),
                        Level.DEBUG,
                        "Could not create .*\\.vec with direct IO"
                    )
                );
            }
            String indexName = indexVectors(false, true);
            indexDoc(indexName, "extra", "fooVector", IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
            refresh();
            assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
            var knn = List.of(new KnnSearchBuilder("fooVector", VectorData.fromBytes(new byte[64]), 10, 20, 10f, null, null));
            assertHitCount(prepareSearch(indexName).setKnnSearch(knn), 10);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.elasticsearch.index.store.FsDirectoryFactory:DEBUG", reason = "to capture trace logging for direct IO")
    public void testDirectIONotUsedForMerges() {
        try (MockLog mockLog = MockLog.capture(FsDirectoryFactory.class)) {
            // with on_disk_merge off a merge never even tries the direct path for the merged raw vector file
            mockLog.addExpectation(
                new MockLog.PatternNotSeenEventExpectation(
                    "Direct IO not attempted for the merge",
                    FsDirectoryFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "Creating .*\\.vec with direct IO"
                )
            );
            String indexName = indexVectors(false, false);
            indexDoc(indexName, "extra", "fooVector", IntStream.range(0, 64).mapToDouble(d -> randomFloat()).toArray());
            refresh();
            assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
            mockLog.assertAllExpectationsMatched();
        }
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false; // we require to always use the "real" hybrid directory
    }
}
