/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.test;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class SecurityTestUtils {

    public static String writeFile(Path folder, String name, byte[] content) {
        final Path path = folder.resolve(name);
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(path.getParent(), path.getFileName().toString(), "tmp");
            try (OutputStream os = Files.newOutputStream(tempFile, CREATE, TRUNCATE_EXISTING, WRITE)) {
                Streams.copy(content, os);
            }

            try {
                Files.move(tempFile, path, REPLACE_EXISTING, ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException e) {
                Files.move(tempFile, path, REPLACE_EXISTING);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format(Locale.ROOT, "could not write file [%s]", path.toAbsolutePath()), e);
        } finally {
            // we are ignoring exceptions here, so we do not need handle whether or not tempFile was initialized nor if the file exists
            IOUtils.deleteFilesIgnoringExceptions(tempFile);
        }
        return path.toAbsolutePath().toString();
    }

    public static String writeFile(Path folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(StandardCharsets.UTF_8));
    }

    public static RoutingTable buildIndexRoutingTable(Index index) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(new ShardId(index, 0)).addShard(
            shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize()).moveToStarted()
        ).build();
        return RoutingTable.builder().add(IndexRoutingTable.builder(index).addIndexShard(table).build()).build();
    }

    /**
     * Adds the index alias {@code .security} to the underlying concrete index.
     */
    public static Metadata addAliasToMetadata(Metadata metadata, String indexName) {
        AliasMetadata aliasMetadata = AliasMetadata.newAliasMetadataBuilder(SECURITY_MAIN_ALIAS).build();
        Metadata.Builder metadataBuilder = Metadata.builder(metadata);
        IndexMetadata indexMetadata = metadata.index(indexName);
        metadataBuilder.put(IndexMetadata.builder(indexMetadata).putAlias(aliasMetadata));
        return metadataBuilder.build();
    }
}
