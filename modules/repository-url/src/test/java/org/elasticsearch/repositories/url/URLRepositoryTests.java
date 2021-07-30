/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.url;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.url.http.URLHttpClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;

public class URLRepositoryTests extends ESTestCase {

    private URLRepository createRepository(Settings baseSettings, RepositoryMetadata repositoryMetadata) {
        return new URLRepository(repositoryMetadata, TestEnvironment.newEnvironment(baseSettings),
            new NamedXContentRegistry(Collections.emptyList()), BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(baseSettings, new ClusterSettings(baseSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(URLHttpClient.Factory.class)) {
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
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
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
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
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
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
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
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("url", URLRepository.TYPE, baseSettings);
        final URLRepository repository = createRepository(baseSettings, repositoryMetadata);
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
