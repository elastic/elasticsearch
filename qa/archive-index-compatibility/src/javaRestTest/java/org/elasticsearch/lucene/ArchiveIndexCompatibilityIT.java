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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;

public class ArchiveIndexCompatibilityIT extends AbstractArchiveIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public ArchiveIndexCompatibilityIT(Version version) {
        super(version);
    }

    public void testArchiveIndicesV5() throws Exception {
        verifyArchiveIndexCompatibility("5");
    }

    public void verifyArchiveIndexCompatibility(String version) throws Exception {
        final String repository = "repository";
        final String snapshot = "snapshot";
        final String index = "index";

        String repositoryPath = REPOSITORY_PATH.getRoot().getPath();

        if (VERSION_MINUS_1.equals(clusterVersion())) {
            assertTrue(getIndices(client()).isEmpty());

            Path resourceFolder = Paths.get(
                Objects.requireNonNull(getClass().getClassLoader().getResource("snapshot_v" + version)).toURI()
            );
            copyFolder(resourceFolder, Paths.get(repositoryPath));

            registerRepository(
                client(),
                repository,
                FsRepository.TYPE,
                true,
                Settings.builder().put("location", repositoryPath).build()
            );

            restoreSnapshot(client(), repository, snapshot, index);
            assertTrue(getIndices(client()).contains(index));

            return;
        }

        if (VERSION_CURRENT.equals(clusterVersion())) {
            assertTrue(getIndices(client()).contains(index));
        }
    }

    private void copyFolder(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path targetDir = target.resolve(source.relativize(dir));
                if (Files.exists(targetDir) == false) {
                    Files.createDirectory(targetDir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, target.resolve(source.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void addDocuments(RestClient client, String index, int numDocs) throws Exception {
        final var bulks = new StringBuilder();
        IntStream.range(0, numDocs).forEach(n -> bulks.append(Strings.format("""
            {"index":{"_id":"%s","_index":"%s"}}
            {"test":"test"}
            """, n, index)));

        var bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulks.toString());
        var bulkResponse = client.performRequest(bulkRequest);
        assertOK(bulkResponse);
    }

    private void restoreSnapshot(RestClient client, String repository, String snapshot, String index) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
            {
              "indices": "%s",
              "include_global_state": false,
              "rename_pattern": "(.+)",
              "include_aliases": false
            }""", index));
        createFromResponse(client.performRequest(request));
    }

    private String getIndices(RestClient client) throws IOException {
        final Request request = new Request("GET", "_cat/indices");
        Response response = client.performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }
}
