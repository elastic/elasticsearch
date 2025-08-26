/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class is responsible for crawling and extracting OpenTelemetry semantic convention
 * resource attributes from the OpenTelemetry GitHub repository. It handles downloading,
 * unzipping, and processing YAML files to extract specific referenced resource attribute names.
 * It eventually deletes the downloaded zip file and extracted repository directory.
 */
public class OTelSemConvCrawler {

    public static final String SEM_CONV_GITHUB_REPO_ZIP_URL =
        "https://github.com/open-telemetry/semantic-conventions/archive/refs/heads/main.zip";

    private static final Logger logger = LogManager.getLogger(OTelSemConvCrawler.class);

    @SuppressForbidden(reason = "writing the GitHub repo zip file to the test's runtime temp directory and deleting on exit")
    static Set<String> collectOTelSemConvResourceAttributes() {
        Path semConvZipFilePath = null;
        Path semConvExtractedTmpDirPath = null;
        Set<String> resourceAttributes = new HashSet<>();
        try {
            HttpClient httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();

            semConvZipFilePath = Files.createTempFile("otel-semconv-", ".zip");

            // Download zip
            HttpResponse<Path> response = httpClient.send(
                HttpRequest.newBuilder(URI.create(SEM_CONV_GITHUB_REPO_ZIP_URL)).build(),
                HttpResponse.BodyHandlers.ofFile(semConvZipFilePath)
            );

            if (response.statusCode() != 200) {
                logger.error("failed to download semantic conventions zip file");
                return resourceAttributes;
            }

            // Unzip
            semConvExtractedTmpDirPath = Files.createTempDirectory("otel-semconv-extracted-");
            try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(semConvZipFilePath))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.isDirectory() == false) {
                        Path outPath = semConvExtractedTmpDirPath.resolve(entry.getName());
                        Files.createDirectories(outPath.getParent());
                        Files.copy(zis, outPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }

            // look for the model root at semantic-conventions-main/model
            Path semConvModelRootDir = semConvExtractedTmpDirPath.resolve("semantic-conventions-main/model");
            if (Files.exists(semConvModelRootDir) == false) {
                logger.error("model directory not found in the extracted zip");
                return resourceAttributes;
            }

            try (Stream<Path> semConvFileStream = Files.walk(semConvModelRootDir)) {
                semConvFileStream.filter(path -> path.toString().endsWith(".yaml") || path.toString().endsWith(".yml"))
                    .parallel()
                    .forEach(path -> {
                        try (
                            InputStream inputStream = Files.newInputStream(path);
                            XContentParser parser = XContentFactory.xContent(XContentType.YAML)
                                .createParser(XContentParserConfiguration.EMPTY, inputStream)
                        ) {
                            Map<String, Object> yamlData = parser.map();
                            Object groupsObj = yamlData.get("groups");
                            if (groupsObj instanceof List<?> groups) {
                                for (Object group : groups) {
                                    if (group instanceof Map<?, ?> groupMap && "entity".equals(groupMap.get("type"))) {
                                        Object attrs = groupMap.get("attributes");
                                        if (attrs instanceof List<?> attrList) {
                                            for (Object attr : attrList) {
                                                if (attr instanceof Map<?, ?> attrMap) {
                                                    String refVal = (String) attrMap.get("ref");
                                                    if (refVal != null) {
                                                        resourceAttributes.add(refVal);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (IOException e) {
                            logger.error("error parsing yaml file", e);
                        }
                    });
            }
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
        } catch (IOException e) {
            logger.error("IO exception", e);
        } finally {
            if (semConvZipFilePath != null) {
                try {
                    Files.deleteIfExists(semConvZipFilePath);
                } catch (IOException e) {
                    logger.warn("failed to delete semconv zip file", e);
                }
            }
            if (semConvExtractedTmpDirPath != null) {
                try (Stream<Path> semConvFileStream = Files.walk(semConvExtractedTmpDirPath)) {
                    semConvFileStream.sorted(Comparator.reverseOrder()) // delete files first
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                logger.warn("failed to delete file: " + path, e);
                            }
                        });
                } catch (IOException e) {
                    logger.warn("failed to delete semconv zip file", e);
                }
            }
        }

        return resourceAttributes;
    }
}
