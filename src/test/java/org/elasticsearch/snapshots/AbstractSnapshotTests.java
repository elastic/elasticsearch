/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.metadata.SnapshotMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Ignore;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 */
@Ignore
public abstract class AbstractSnapshotTests extends ElasticsearchIntegrationTest {

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static int numberOfFiles(Path dir) throws IOException {
        final AtomicInteger count = new AtomicInteger();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                count.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });
        return count.get();
    }

    public static void stopNode(final String node) throws IOException {
        internalCluster().stopRandomNode(new Predicate<Settings>() {
            @Override
            public boolean apply(Settings settings) {
                return settings.get("name").equals(node);
            }
        });
    }

    public void waitForBlock(String node, String repository, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, node);
        MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            if (mockRepository.blocked()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
    }

    public SnapshotInfo waitForCompletion(String repository, String snapshot, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        SnapshotId snapshotId = new SnapshotId(repository, snapshot);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            ImmutableList<SnapshotInfo> snapshotInfos = client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot).get().getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                // Make sure that snapshot clean up operations are finished
                ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
                SnapshotMetaData snapshotMetaData = stateResponse.getState().getMetaData().custom(SnapshotMetaData.TYPE);
                if (snapshotMetaData == null || snapshotMetaData.snapshot(snapshotId) == null) {
                    return snapshotInfos.get(0);
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }

    public static String blockNodeWithIndex(String index) {
        for(String node : internalCluster().nodesInclude("test-idx")) {
            ((MockRepository)internalCluster().getInstance(RepositoriesService.class, node).repository("test-repo")).blockOnDataFiles(true);
            return node;
        }
        fail("No nodes for the index " + index + " found");
        return null;
    }

    public static void unblockNode(String node) {
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, node).repository("test-repo")).unblock();
    }
}
