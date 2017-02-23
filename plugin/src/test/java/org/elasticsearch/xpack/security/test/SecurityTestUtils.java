/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStoreTests;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;

import static org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE;

public class SecurityTestUtils {

    public static Path createFolder(Path parent, String name) {
        Path createdFolder = parent.resolve(name);
        //the directory might exist e.g. if the global cluster gets restarted, then we recreate the directory as well
        if (Files.exists(createdFolder)) {
            try {
                FileSystemUtils.deleteSubDirectories(createdFolder);
            } catch (IOException e) {
                throw new RuntimeException("could not delete existing temporary folder: " + createdFolder.toAbsolutePath(), e);
            }
        } else {
            try {
                Files.createDirectories(createdFolder);
            } catch (IOException e) {
                throw new RuntimeException("could not create temporary folder: " + createdFolder.toAbsolutePath());
            }
        }
        return createdFolder;
    }

    public static String writeFile(Path folder, String name, byte[] content) {
        Path file = folder.resolve(name);
        try (OutputStream os = Files.newOutputStream(file)) {
            Streams.copy(content, os);
        } catch (IOException e) {
            throw new ElasticsearchException("error writing file in test", e);
        }
        return file.toAbsolutePath().toString();
    }

    public static String writeFile(Path folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(StandardCharsets.UTF_8));
    }

    public static ClusterState getClusterStateWithSecurityIndex() {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder(SecurityLifecycleService.SECURITY_INDEX_NAME).settings(settings))
                .put(new IndexTemplateMetaData(SecurityLifecycleService.SECURITY_TEMPLATE_NAME, 0, 0,
                        Collections.singletonList(SecurityLifecycleService.SECURITY_INDEX_NAME), Settings.EMPTY, ImmutableOpenMap.of(),
                        ImmutableOpenMap.of(), ImmutableOpenMap.of()))
                .build();
        RoutingTable routingTable = buildSecurityIndexRoutingTable();

        return ClusterState.builder(new ClusterName(NativeRolesStoreTests.class.getName()))
                .metaData(metaData)
                .routingTable(routingTable)
                .build();
    }

    public static RoutingTable buildSecurityIndexRoutingTable() {
        Index index = new Index(SecurityLifecycleService.SECURITY_INDEX_NAME, UUID.randomUUID().toString());
        ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId(index, 0), true, EXISTING_STORE_INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        String nodeId = ESTestCase.randomAsciiOfLength(8);
        IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize()).moveToStarted())
                .build();
        return RoutingTable.builder()
                .add(IndexRoutingTable
                        .builder(index)
                        .addIndexShard(table)
                        .build())
                .build();
    }
}
