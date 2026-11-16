/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A file-based catalog in the repository: one JSON file for each table, {@code <base path>/_catalog/<namespace>/<name>.json}, see
 * {@link CatalogEntry}. The catalog maps a stable name to the current location of a table, so that a reader finds a table by its name and
 * not by guessing a path. There is no service. The directory name starts with an underscore, which no index name can, so it cannot clash
 * with a table path ({@code <index name>/<index uuid>}).
 *
 * <p>The table commit is not here: it is the put-if-absent of {@code v<N+1>.metadata.json}, and the entry is written after it, the way the
 * version hint is. Two shards that commit at the same moment can leave an entry that is one version behind, which the next commit repairs,
 * and {@link #load} gives a reader the newest table anyway by walking forward from the entry.
 */
public final class RepositoryCatalog {

    private static final Logger logger = LogManager.getLogger(RepositoryCatalog.class);

    /** The directory of the catalog below the base path of the repository. */
    public static final String DIRECTORY = "_catalog";

    private static final int ATTEMPTS = 3;

    private final BlobStore blobStore;
    private final BlobPath catalogPath;
    /** Maps the URIs below the base path of the repository to blobs, so that {@link #load} can look for the next metadata file. */
    private final RepositoryFileIO io;

    /**
     * @param repositoryBasePath the blob path of the base path of the repository
     * @param repositoryBaseUri  the URI that the base path has outside Elasticsearch (the table URIs are below it)
     */
    public RepositoryCatalog(BlobStore blobStore, BlobPath repositoryBasePath, String repositoryBaseUri) {
        this.blobStore = blobStore;
        this.catalogPath = repositoryBasePath.add(DIRECTORY);
        this.io = new RepositoryFileIO(blobStore, repositoryBasePath, repositoryBaseUri);
    }

    private BlobContainer container(List<String> namespace) {
        BlobPath path = catalogPath;
        for (String level : namespace) {
            path = path.add(level);
        }
        return blobStore.blobContainer(path);
    }

    private static String blobName(String name) {
        return name + ".json";
    }

    /** The entry as it was written, or empty when the table has none. */
    public Optional<CatalogEntry> read(List<String> namespace, String name) throws IOException {
        try (InputStream in = container(namespace).readBlob(RepositoryFileIO.PURPOSE, blobName(name))) {
            return Optional.of(CatalogEntry.parse(new BytesArray(in.readAllBytes())));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    /**
     * Creates the entry of the table, or updates it when it exists:
     * <ul>
     *   <li>an update keeps the creation time and records the metadata file that it replaces as the previous one;</li>
     *   <li>an entry that is already at this version or a newer one of the same table is left alone, so a shard that is late does not
     *       move the pointer back;</li>
     *   <li>an entry of another table with the same name (an index that was deleted and created again, or another cluster) is replaced
     *       by the new one, with a warning.</li>
     * </ul>
     */
    public void register(CatalogEntry entry) throws IOException {
        final BlobContainer container = container(entry.namespace());
        IOException failure = null;
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            final Optional<CatalogEntry> existing = read(entry.namespace(), entry.name());
            CatalogEntry toWrite = entry;
            if (existing.isPresent()) {
                final CatalogEntry old = existing.get();
                if (old.location().equals(entry.location())) {
                    if (old.metadataVersion() >= entry.metadataVersion()) {
                        return;
                    }
                    toWrite = entry.withCreatedAt(old.createdAtMillis())
                        .withMetadata(entry.metadataLocation(), entry.metadataVersion(), old.metadataLocation(), entry.updatedAtMillis());
                } else {
                    logger.warn(
                        "the catalog name [{}] in the namespace {} named the table [{}] of the cluster [{}], and now names [{}]",
                        entry.name(),
                        entry.namespace(),
                        old.location(),
                        old.clusterUuid(),
                        entry.location()
                    );
                }
            }
            try {
                // A create must not replace an entry that another shard has just written; if it would, we read it again and update.
                container.writeBlobAtomic(RepositoryFileIO.PURPOSE, blobName(entry.name()), toWrite.toBytes(), existing.isEmpty());
                return;
            } catch (FileAlreadyExistsException e) {
                failure = e;
            } catch (IOException e) {
                // an S3 repository reports a conditional write that lost as a plain IOException
                failure = e;
            }
        }
        throw failure;
    }

    /**
     * The entry of the table with the newest metadata file: it reads the entry, and then follows {@code v<N+1>.metadata.json} while that
     * file exists, so that an entry that lags (see the class comment) never gives a reader an older table.
     */
    public Optional<CatalogEntry> load(List<String> namespace, String name) throws IOException {
        final Optional<CatalogEntry> read = read(namespace, name);
        if (read.isEmpty()) {
            return read;
        }
        final CatalogEntry entry = read.get();
        int version = entry.metadataVersion();
        String location = entry.metadataLocation();
        final String directory = location.substring(0, location.lastIndexOf('/') + 1);
        while (true) {
            final String next = directory + "v" + (version + 1) + ".metadata.json";
            if (io.containerOf(next).blobExists(RepositoryFileIO.PURPOSE, io.blobNameOf(next)) == false) {
                break;
            }
            version++;
            location = next;
        }
        return Optional.of(
            version == entry.metadataVersion()
                ? entry
                : entry.withMetadata(location, version, directory + "v" + (version - 1) + ".metadata.json", entry.updatedAtMillis())
        );
    }

    /** The names of the tables of a namespace, sorted. */
    public List<String> list(List<String> namespace) throws IOException {
        final List<String> names = new ArrayList<>();
        for (String blob : container(namespace).listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            if (blob.endsWith(".json")) {
                names.add(blob.substring(0, blob.length() - ".json".length()));
            }
        }
        Collections.sort(names);
        return names;
    }

    /** The names of the namespaces directly below a namespace (below the catalog for an empty list), sorted. */
    public List<String> listNamespaces(List<String> parent) throws IOException {
        final List<String> names = new ArrayList<>(container(parent).children(RepositoryFileIO.PURPOSE).keySet());
        Collections.sort(names);
        return names;
    }
}
