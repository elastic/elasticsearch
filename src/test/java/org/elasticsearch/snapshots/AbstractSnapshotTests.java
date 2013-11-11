/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

/**
 */
@Ignore
public abstract class AbstractSnapshotTests extends ElasticsearchIntegrationTest {


    @After
    public final void wipeAfter() {
        wipeRepositories();
    }

    @Before
    public final void wipeBefore() {
        wipeRepositories();
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public static void wipeRepositories(String... repositories) {
        // if nothing is provided, delete all
        if (repositories.length == 0) {
            repositories = new String[]{"*"};
        }
        for (String repository : repositories) {
            try {
                client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
            } catch (RepositoryMissingException ex) {
                // ignore
            }
        }
    }

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService : cluster().getInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static int numberOfFiles(File dir) {
        int count = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    count += numberOfFiles(file);
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    public static void stopNode(final String node) {
        cluster().stopRandomNode(new Predicate<Settings>() {
            @Override
            public boolean apply(Settings settings) {
                return settings.get("name").equals(node);
            }
        });
    }

    public String waitForCompletionOrBlock(Collection<String> nodes, String repository, String snapshot, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.millis()) {
            ImmutableList<SnapshotInfo> snapshotInfos = run(client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot)).getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                return null;
            }
            for (String node : nodes) {
                RepositoriesService repositoriesService = cluster().getInstance(RepositoriesService.class, node);
                if (((MockRepository) repositoriesService.repository(repository)).blocked()) {
                    return node;
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }

    public SnapshotInfo waitForCompletion(String repository, String snapshot, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.millis()) {
            ImmutableList<SnapshotInfo> snapshotInfos = run(client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshot)).getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                return snapshotInfos.get(0);
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }

    public static void unblock(String repository) {
        for (RepositoriesService repositoriesService : cluster().getInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).unblock();
        }
    }
}
