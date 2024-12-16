/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;

/**
 * The test suite creates a cluster in the N-1 version, where N is the current version.
 * Mounts snapshots from old-clusters (version 5/6) and upgrades it to the current version.
 * Test methods are executed after each upgrade.
 */
public class SearchableSnapshotUpgradeCompatibilityIT extends AbstractArchiveIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public SearchableSnapshotUpgradeCompatibilityIT(Version version) {
        super(version);
    }

    public void testSearchableSnapshotV5() throws Exception {
        verifySearchableSnapshotCompatibility("5");
    }

    public void verifySearchableSnapshotCompatibility(String version) throws Exception {
        final String repository = "repository";
        final String snapshot = "snapshot";
        final String index = "index";
        String mountedIndex = "mounted_index";

        String repositoryPath = REPOSITORY_PATH.getRoot().getPath();

        if (VERSION_MINUS_1.equals(clusterVersion())) {
            assertTrue(getIndices(client()).isEmpty());

            // Copy a snapshot of an index with 5 documents
            copySnapshotFromResources(repositoryPath, version);
            registerRepository(client(), repository, FsRepository.TYPE, true, Settings.builder().put("location", repositoryPath).build());
            mountSnapshot(client(), repository, snapshot, index, mountedIndex);

            assertTrue(getIndices(client()).contains(mountedIndex));
            return;
        }

        if (VERSION_CURRENT.equals(clusterVersion())) {
            assertTrue(getIndices(client()).contains(mountedIndex));
        }
    }

    private void copySnapshotFromResources(String repositoryPath, String version) throws IOException, URISyntaxException {
        Path zipFilePath = Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("snapshot_v" + version + ".zip")).toURI()
        );
        unzip(zipFilePath, Paths.get(repositoryPath));
    }

    public static void unzip(Path zipFilePath, Path outputDir) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipFilePath))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path outputPath = outputDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(outputPath);
                } else {
                    Files.createDirectories(outputPath.getParent());
                    try (OutputStream out = Files.newOutputStream(outputPath)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zipIn.read(buffer)) > 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipIn.closeEntry();
            }
        }
    }

    private void mountSnapshot(RestClient client, String repository, String snapshot, String index, String mountedIndex) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_mount");
        request.addParameter("wait_for_completion", "true");
        var storage = randomBoolean() ? "shared_cache" : "full_copy";
        request.addParameter("storage", storage);
        request.setJsonEntity(Strings.format("""
             {
              "index": "%s",
              "renamed_index": "%s"
            }""", index, mountedIndex));
        createFromResponse(client.performRequest(request));
    }

    private String getIndices(RestClient client) throws IOException {
        final Request request = new Request("GET", "_cat/indices");
        Response response = client.performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }
}
