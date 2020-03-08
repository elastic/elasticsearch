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

package org.elasticsearch.repositories.url;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class URLRepositoryTests extends ESTestCase {

    private URLRepository createRepository(Settings baseSettings, RepositoryMetaData repositoryMetaData) {
        return new URLRepository(repositoryMetaData, TestEnvironment.newEnvironment(baseSettings),
            new NamedXContentRegistry(Collections.emptyList()), BlobStoreTestUtil.mockClusterService()) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually on test/main threads
            }
        };
    }

    public void testWhiteListingRepoURL() throws IOException {
        String repoPath = createTempDir().resolve("repository").toUri().toURL().toString();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(URLRepository.ALLOWED_URLS_SETTING.getKey(), repoPath)
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), repoPath)
            .build();
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetaData);
        repository.start();

        assertThat("blob store has to be lazy initialized", repository.getBlobStore(), is(nullValue()));
        repository.blobContainer();
        assertThat("blobContainer has to initialize blob store", repository.getBlobStore(), not(nullValue()));
    }

    public void testIfNotWhiteListedMustSetRepoURL() throws IOException {
        String repoPath = createTempDir().resolve("repository").toUri().toURL().toString();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), repoPath)
            .build();
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetaData);
        repository.start();
        try {
            repository.blobContainer();
            fail("RepositoryException should have been thrown.");
        } catch (RepositoryException e) {
            String msg = "[url] file url [" + repoPath
                + "] doesn't match any of the locations specified by path.repo or repositories.url.allowed_urls";
            assertEquals(msg, e.getMessage());
        }
    }

    public void testMustBeSupportedProtocol() throws IOException {
        Path directory = createTempDir();
        String repoPath = directory.resolve("repository").toUri().toURL().toString();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(Environment.PATH_REPO_SETTING.getKey(), directory.toString())
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), repoPath)
            .put(URLRepository.SUPPORTED_PROTOCOLS_SETTING.getKey(), "http,https")
            .build();
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetaData);
        repository.start();
        try {
            repository.blobContainer();
            fail("RepositoryException should have been thrown.");
        } catch (RepositoryException e) {
            assertEquals("[url] unsupported url protocol [file] from URL [" + repoPath +"]", e.getMessage());
        }
    }

    public void testNonNormalizedUrl() throws IOException {
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(URLRepository.ALLOWED_URLS_SETTING.getKey(), "file:/tmp/")
            .put(URLRepository.REPOSITORIES_URL_SETTING.getKey(), "file:/var/" )
            .build();
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetaData);
        repository.start();
        try {
            repository.blobContainer();
            fail("RepositoryException should have been thrown.");
        } catch (RepositoryException e) {
            assertEquals("[url] file url [file:/var/] doesn't match any of the locations "
                + "specified by path.repo or repositories.url.allowed_urls",
                e.getMessage());
        }
    }

}
