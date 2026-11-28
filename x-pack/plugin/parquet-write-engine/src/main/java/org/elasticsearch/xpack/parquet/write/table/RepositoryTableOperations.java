/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The commit protocol of an Iceberg table that lives in the blob store of a snapshot repository, with the layout that the ES|QL Iceberg
 * data source already reads: {@code metadata/v<N>.metadata.json} and {@code metadata/version-hint.text}.
 *
 * <p>The table has no catalog. The current version is the highest {@code v<N>.metadata.json} that a listing of {@code metadata/} shows,
 * and a commit writes {@code v<N+1>.metadata.json} with an atomic put-if-absent. A writer that loses the race gets a
 * {@link CommitFailedException}, and Iceberg then refreshes and applies its change again, so shards can commit to the same table
 * concurrently with no coordination. The version hint is only a pointer for readers that do not list; it is written after the commit
 * with an overwrite, and a reader must never trust it over the listing.
 *
 * <p>A table that another tool wrote through a catalog (pyiceberg, Spark) names its metadata files {@code <NNNNN>-<uuid>.metadata.json}.
 * Such a table can be read: its current version is the highest number that the listing shows, plus one, so that the version of a table
 * that exists is never 0. It is never committed to, because the catalog of that tool decides which file is current, and this class does
 * not know it. A metadata file of a failed catalog commit with a higher number than the current one would be read as current.
 */
public final class RepositoryTableOperations implements TableOperations {

    private static final Logger logger = LogManager.getLogger(RepositoryTableOperations.class);

    private static final Pattern VERSION_FILE = Pattern.compile("v(\\d+)\\.metadata\\.json");
    /** The name that Iceberg's catalogs give a metadata file: the sequence number of the version and a random uuid. */
    private static final Pattern CATALOG_FILE = Pattern.compile("(\\d+)-[0-9a-fA-F-]+(?:\\.gz)?\\.metadata\\.json");
    private static final String VERSION_HINT = "version-hint.text";
    private static final String PREVIOUS_VERSIONS_MAX = "write.metadata.previous-versions-max";
    private static final int DEFAULT_PREVIOUS_VERSIONS_MAX = 100;

    private final RepositoryFileIO io;
    private final String tableLocation;
    /** Where to write the entry of the table in the catalog after a commit, or null for a table that is not in a catalog. */
    @Nullable
    private final CatalogRegistration registration;
    private TableMetadata current;
    private boolean shouldRefresh = true;

    public RepositoryTableOperations(RepositoryFileIO io) {
        this(io, null);
    }

    public RepositoryTableOperations(RepositoryFileIO io, @Nullable CatalogRegistration registration) {
        this.io = io;
        this.tableLocation = io.rootLocation();
        this.registration = registration;
    }

    private String metadataLocation(String fileName) {
        return tableLocation + "/metadata/" + fileName;
    }

    private BlobContainer metadataContainer() {
        // any location below metadata/ gives the container of that directory
        return io.containerOf(metadataLocation(VERSION_HINT));
    }

    /**
     * A metadata file of the table.
     *
     * @param version     the version that the name gives (see {@link #versionOfMetadataFile})
     * @param name        the name of the file in {@code metadata/}
     * @param catalogFile whether a catalog named the file ({@code <NNNNN>-<uuid>.metadata.json}), rather than this class
     */
    private record MetadataFile(int version, String name, boolean catalogFile) {}

    /**
     * The version of a table that the name of a metadata file gives: {@code N} for {@code v<N>.metadata.json}, and {@code N + 1} for the
     * {@code <N>-<uuid>.metadata.json} of a catalog, whose first version is 0.
     *
     * @return the version, or -1 if the name is not the name of a metadata file
     */
    public static int versionOfMetadataFile(String fileName) {
        final Matcher versionFile = VERSION_FILE.matcher(fileName);
        if (versionFile.matches()) {
            return Integer.parseInt(versionFile.group(1));
        }
        final Matcher catalogFile = CATALOG_FILE.matcher(fileName);
        if (catalogFile.matches()) {
            return Integer.parseInt(catalogFile.group(1)) + 1;
        }
        return -1;
    }

    /** The metadata file with the highest version in the listing of {@code metadata/}, or null when the table does not exist yet. */
    @Nullable
    private MetadataFile latestMetadata() {
        final Map<String, BlobMetadata> blobs;
        try {
            // all of metadata/ and not the prefix "v": the files that a catalog names start with their number
            blobs = metadataContainer().listBlobs(RepositoryFileIO.PURPOSE);
        } catch (IOException e) {
            throw new RuntimeIOException(e, "failed to list the metadata of [%s]", tableLocation);
        }
        MetadataFile latest = null;
        for (String name : blobs.keySet()) {
            final int version = versionOfMetadataFile(name);
            if (version > 0 && (latest == null || version > latest.version())) {
                latest = new MetadataFile(version, name, VERSION_FILE.matcher(name).matches() == false);
            }
        }
        return latest;
    }

    /** The highest version in the listing of {@code metadata/}, or 0 when the table does not exist yet. */
    private int latestVersion() {
        final MetadataFile latest = latestMetadata();
        return latest == null ? 0 : latest.version();
    }

    private TableMetadata load(MetadataFile file) {
        return TableMetadataParser.read(io, metadataLocation(file.name()));
    }

    @Override
    public TableMetadata current() {
        if (shouldRefresh) {
            return refresh();
        }
        return current;
    }

    @Override
    public TableMetadata refresh() {
        final MetadataFile latest = latestMetadata();
        try {
            current = latest == null ? null : load(latest);
        } catch (NotFoundException e) {
            // the listing and the read are not one operation, and a version can be written between them: that is not an error
            current = null;
            throw e;
        }
        shouldRefresh = false;
        return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        if (base == metadata) {
            return;
        }
        final MetadataFile latestFile = latestMetadata();
        if (latestFile != null && latestFile.catalogFile()) {
            // not a CommitFailedException, which Iceberg retries: the table belongs to a catalog, and no retry changes that
            throw new IllegalStateException(
                "cannot commit to [" + tableLocation + "]: its metadata file [" + latestFile.name() + "] was written through a catalog"
            );
        }
        final int latestVersion = latestFile == null ? 0 : latestFile.version();
        final TableMetadata latest = latestFile == null ? null : load(latestFile);
        if (base == null) {
            if (latest != null) {
                throw new CommitFailedException("cannot create the table [%s]: it exists", tableLocation);
            }
        } else if (latest == null || latest.metadataFileLocation().equals(base.metadataFileLocation()) == false) {
            throw new CommitFailedException("cannot commit to [%s]: the base metadata is not the current metadata", tableLocation);
        }

        final int nextVersion = latestVersion + 1;
        final String location = metadataLocation("v" + nextVersion + ".metadata.json");
        final byte[] json = TableMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8);
        final BlobContainer container = metadataContainer();
        final String blobName = io.blobNameOf(location);
        try {
            // An atomic put-if-absent: of several writers that read the same latest version, exactly one wins.
            container.writeBlobAtomic(RepositoryFileIO.PURPOSE, blobName, new BytesArray(json), true);
        } catch (FileAlreadyExistsException e) {
            throw new CommitFailedException(e, "version [%s] of [%s] was committed by another writer", nextVersion, tableLocation);
        } catch (IOException e) {
            resolveUnclearWrite(container, blobName, json, e, nextVersion);
        }
        writeVersionHint(nextVersion);
        registerInCatalog(metadata);
        deleteOldVersions(nextVersion, metadata.properties());
        shouldRefresh = true;
    }

    /**
     * Deletes the metadata files of versions older than the current one by more than {@code write.metadata.previous-versions-max}
     * (100 by default, as in Iceberg): every commit writes a file, and a reader needs only the latest. A failure is not an error, the
     * next commit tries again.
     */
    private void deleteOldVersions(int latest, Map<String, String> properties) {
        final String configured = properties.get(PREVIOUS_VERSIONS_MAX);
        final int keep = configured == null ? DEFAULT_PREVIOUS_VERSIONS_MAX : Integer.parseInt(configured);
        try {
            final BlobContainer container = metadataContainer();
            final List<String> old = new ArrayList<>();
            for (String name : container.listBlobsByPrefix(RepositoryFileIO.PURPOSE, "v").keySet()) {
                final Matcher matcher = VERSION_FILE.matcher(name);
                if (matcher.matches() && Integer.parseInt(matcher.group(1)) < latest - keep) {
                    old.add(name);
                }
            }
            if (old.isEmpty() == false) {
                container.deleteBlobsIgnoringIfNotExists(RepositoryFileIO.PURPOSE, old.iterator());
            }
        } catch (IOException e) {
            // best effort
        }
    }

    /**
     * The S3 repository reports a conditional write that lost the race (HTTP 412) as an {@code IOException} that wraps an SDK exception,
     * and not as a {@link FileAlreadyExistsException} like the file system repository does. A write whose response is lost after the
     * server stored the blob looks the same. So after any other {@code IOException} we look at the blob: it is missing (a real failure),
     * it holds our bytes (we won, and the commit is done), or it holds the bytes of another writer (we lost, and Iceberg retries).
     */
    private void resolveUnclearWrite(BlobContainer container, String blobName, byte[] json, IOException failure, int version) {
        final byte[] existing;
        try {
            if (container.blobExists(RepositoryFileIO.PURPOSE, blobName) == false) {
                throw new RuntimeIOException(failure, "failed to write version [%s] of [%s]", version, tableLocation);
            }
            try (InputStream in = container.readBlob(RepositoryFileIO.PURPOSE, blobName)) {
                existing = in.readAllBytes();
            }
        } catch (IOException readFailure) {
            failure.addSuppressed(readFailure);
            throw new RuntimeIOException(failure, "failed to write version [%s] of [%s]", version, tableLocation);
        }
        if (Arrays.equals(existing, json) == false) {
            throw new CommitFailedException(failure, "version [%s] of [%s] was committed by another writer", version, tableLocation);
        }
    }

    /**
     * Creates or updates the entry of the table in the catalog, after the commit and the version hint. The entry names the newest version
     * that the listing shows, which can be a version that another shard has committed since ours. A failure is logged and does not fail
     * the commit: the table is committed, and the next commit writes the entry again.
     */
    private void registerInCatalog(TableMetadata metadata) {
        if (registration == null) {
            return;
        }
        try {
            final int latest = latestVersion();
            final long now = System.currentTimeMillis();
            registration.catalog()
                .register(
                    new CatalogEntry(
                        registration.namespace(),
                        registration.name(),
                        metadata.uuid(),
                        metadata.location(),
                        metadataLocation("v" + latest + ".metadata.json"),
                        null,
                        latest,
                        now,
                        now,
                        registration.index(),
                        registration.indexUuid(),
                        registration.clusterUuid()
                    )
                );
        } catch (Exception e) {
            logger.warn(
                "failed to write the catalog entry [{}] of the table [{}]; the next commit writes it again",
                registration.name(),
                tableLocation,
                e
            );
        }
    }

    /** A pointer for readers that do not list. A failure here does not undo the commit, which is already visible in the listing. */
    private void writeVersionHint(int version) {
        try {
            metadataContainer().writeBlobAtomic(
                RepositoryFileIO.PURPOSE,
                VERSION_HINT,
                new BytesArray(Integer.toString(version).getBytes(StandardCharsets.UTF_8)),
                false
            );
        } catch (IOException e) {
            // best effort: the listing is the truth
        }
    }

    @Override
    public FileIO io() {
        return io;
    }

    @Override
    public String metadataFileLocation(String fileName) {
        return metadataLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
        final TableMetadata metadata = current();
        return LocationProviders.locationsFor(metadata.location(), metadata.properties());
    }
}
