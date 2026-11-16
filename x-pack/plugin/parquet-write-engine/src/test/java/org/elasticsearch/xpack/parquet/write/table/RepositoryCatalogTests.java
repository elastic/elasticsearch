/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RepositoryCatalogTests extends ESTestCase {

    private static final String BASE_URI = "file:///warehouse";
    private static final List<String> NAMESPACE = List.of("default");

    private FsBlobStore store;
    private RepositoryCatalog catalog;

    @Before
    public void createStore() throws Exception {
        store = new FsBlobStore(4096, createTempDir(), false);
        catalog = new RepositoryCatalog(store, BlobPath.EMPTY, BASE_URI);
    }

    @After
    public void closeStore() throws Exception {
        store.close();
    }

    /** The namespaces, without the extra files that the test file system adds to every directory. */
    private List<String> namespaces(List<String> parent) throws IOException {
        return catalog.listNamespaces(parent).stream().filter(name -> name.startsWith("extra") == false).toList();
    }

    private static String tableRoot(String index) {
        return BASE_URI + "/" + index + "/uuid-" + index;
    }

    private static CatalogEntry entry(String index, int version, long time) {
        final String root = tableRoot(index);
        return new CatalogEntry(
            NAMESPACE,
            index,
            "table-" + index,
            root,
            root + "/metadata/v" + version + ".metadata.json",
            null,
            version,
            time,
            time,
            index,
            "uuid-" + index,
            "cluster"
        );
    }

    /** Writes the metadata files v1 to vN of a table, as the commits of the table would. */
    private void writeMetadataFiles(String index, int newest) throws IOException {
        final var container = store.blobContainer(BlobPath.EMPTY.add(index).add("uuid-" + index).add("metadata"));
        for (int v = 1; v <= newest; v++) {
            container.writeBlob(RepositoryFileIO.PURPOSE, "v" + v + ".metadata.json", new BytesArray("{}"), true);
        }
    }

    public void testATableWithoutAnEntryIsNotThere() throws Exception {
        // the file system store makes a directory when it makes a container, so list the namespaces first
        assertThat(namespaces(List.of()), empty());
        assertThat(catalog.read(NAMESPACE, "logs"), equalTo(Optional.empty()));
        assertThat(catalog.load(NAMESPACE, "logs"), equalTo(Optional.empty()));
        assertThat(catalog.list(NAMESPACE), empty());
    }

    public void testTheFirstRegisterCreatesAndTheNextOnesUpdate() throws Exception {
        catalog.register(entry("logs", 1, 1_000));
        final CatalogEntry first = catalog.read(NAMESPACE, "logs").orElseThrow();
        assertThat(first.metadataVersion(), equalTo(1));
        assertThat(first.createdAtMillis(), equalTo(1_000L));
        assertThat(first.previousMetadataLocation(), nullValue());

        catalog.register(entry("logs", 2, 2_000));
        final CatalogEntry second = catalog.read(NAMESPACE, "logs").orElseThrow();
        assertThat(second.metadataVersion(), equalTo(2));
        assertThat("an update keeps the time of the creation", second.createdAtMillis(), equalTo(1_000L));
        assertThat(second.updatedAtMillis(), equalTo(2_000L));
        assertThat(second.previousMetadataLocation(), equalTo(tableRoot("logs") + "/metadata/v1.metadata.json"));
    }

    public void testAnOlderVersionDoesNotMoveThePointerBack() throws Exception {
        catalog.register(entry("logs", 5, 5_000));

        catalog.register(entry("logs", 4, 6_000));
        catalog.register(entry("logs", 5, 7_000));

        final CatalogEntry kept = catalog.read(NAMESPACE, "logs").orElseThrow();
        assertThat(kept.metadataVersion(), equalTo(5));
        assertThat("the entry that was already there stays as it was", kept.updatedAtMillis(), equalTo(5_000L));
    }

    public void testANameThatIsBoundToAnotherTableIsRebound() throws Exception {
        catalog.register(entry("logs", 9, 1_000));
        // the index was deleted and created again: the same name, another table
        final CatalogEntry again = new CatalogEntry(
            NAMESPACE,
            "logs",
            "another-table",
            BASE_URI + "/logs/uuid-new",
            BASE_URI + "/logs/uuid-new/metadata/v1.metadata.json",
            null,
            1,
            2_000,
            2_000,
            "logs",
            "uuid-new",
            "cluster"
        );

        catalog.register(again);

        final CatalogEntry now = catalog.read(NAMESPACE, "logs").orElseThrow();
        assertThat(now.location(), equalTo(BASE_URI + "/logs/uuid-new"));
        assertThat(now.metadataVersion(), equalTo(1));
        assertThat("the new table has its own creation time", now.createdAtMillis(), equalTo(2_000L));
        assertThat(now.previousMetadataLocation(), nullValue());
    }

    public void testLoadWalksForwardWhileTheNextMetadataFileExists() throws Exception {
        writeMetadataFiles("logs", 4);
        catalog.register(entry("logs", 1, 1_000)); // the entry lags: the table is at version 4

        final CatalogEntry loaded = catalog.load(NAMESPACE, "logs").orElseThrow();

        assertThat(loaded.metadataVersion(), equalTo(4));
        assertThat(loaded.metadataLocation(), equalTo(tableRoot("logs") + "/metadata/v4.metadata.json"));
        assertThat(loaded.previousMetadataLocation(), equalTo(tableRoot("logs") + "/metadata/v3.metadata.json"));
        assertThat("load does not write", catalog.read(NAMESPACE, "logs").orElseThrow().metadataVersion(), equalTo(1));
    }

    public void testLoadOfAnEntryThatIsCurrentReturnsItAsItIs() throws Exception {
        writeMetadataFiles("logs", 3);
        catalog.register(entry("logs", 3, 3_000));

        assertThat(catalog.load(NAMESPACE, "logs").orElseThrow(), equalTo(catalog.read(NAMESPACE, "logs").orElseThrow()));
    }

    public void testTablesAndNamespacesAreListed() throws Exception {
        catalog.register(entry("b-logs", 1, 1));
        catalog.register(entry("a-logs", 1, 1));
        final CatalogEntry nested = new CatalogEntry(
            List.of("prod", "eu"),
            "metrics",
            "t",
            BASE_URI + "/metrics/u",
            BASE_URI + "/metrics/u/metadata/v1.metadata.json",
            null,
            1,
            1,
            1,
            "metrics",
            "u",
            "cluster"
        );
        catalog.register(nested);

        assertThat(catalog.list(NAMESPACE), contains("a-logs", "b-logs"));
        assertThat(catalog.list(List.of("prod", "eu")), contains("metrics"));
        assertThat(namespaces(List.of()), contains("default", "prod"));
        assertThat(namespaces(List.of("prod")), contains("eu"));
    }

    public void testWritersThatRegisterAtTheSameTimeLeaveAValidEntryThatLoadResolvesToTheNewestTable() throws Exception {
        final int writers = between(3, 8);
        writeMetadataFiles("logs", writers);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        final List<Exception> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int w = 1; w <= writers; w++) {
            final int version = w;
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    catalog.register(entry("logs", version, 1_000L * version));
                } catch (Exception e) {
                    failures.add(e);
                }
            });
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(failures, empty());
        final CatalogEntry written = catalog.read(NAMESPACE, "logs").orElseThrow();
        assertTrue("a valid entry of one of the writers", written.metadataVersion() >= 1 && written.metadataVersion() <= writers);
        // the entry may lag, which is the reason for the walk: a reader still gets the newest table
        assertThat(catalog.load(NAMESPACE, "logs").orElseThrow().metadataVersion(), equalTo(writers));
    }
}
