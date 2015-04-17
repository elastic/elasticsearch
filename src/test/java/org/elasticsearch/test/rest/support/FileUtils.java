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
package org.elasticsearch.test.rest.support;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class FileUtils {

    private static final String YAML_SUFFIX = ".yaml";
    private static final String JSON_SUFFIX = ".json";

    private FileUtils() {

    }

    /**
     * Returns the json files found within the directory provided as argument.
     * Files are looked up in the classpath first, then outside of it if not found.
     */
    public static Set<Path> findJsonSpec(String optionalPathPrefix, String path) throws IOException {
        Path dir = resolveFile(optionalPathPrefix, path, null);

        if (!Files.isDirectory(dir)) {
            throw new NotDirectoryException(path);
        }

        Set<Path> jsonFiles = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path item : stream) {
                if (item.toString().endsWith(JSON_SUFFIX)) {
                    jsonFiles.add(item);
                }
            }
        }

        if (jsonFiles.isEmpty()) {
            throw new NoSuchFileException(path, null, "no json files found");
        }

        return jsonFiles;
    }

    /**
     * Returns the yaml files found within the paths provided.
     * Each input path can either be a single file (the .yaml suffix is optional) or a directory.
     * Each path is looked up in the classpath first, then outside of it if not found yet.
     */
    public static Map<String, Set<Path>> findYamlSuites(final String optionalPathPrefix, final String... paths) throws IOException {
        Map<String, Set<Path>> yamlSuites = Maps.newHashMap();
        for (String path : paths) {
            collectFiles(resolveFile(optionalPathPrefix, path, YAML_SUFFIX), YAML_SUFFIX, yamlSuites);
        }
        return yamlSuites;
    }

    private static Path resolveFile(String optionalPathPrefix, String path, String optionalFileSuffix) throws IOException {
        //try within classpath with and without file suffix (as it could be a single test suite)
        URL resource = findResource(path, optionalFileSuffix);
        if (resource == null) {
            //try within classpath with optional prefix: /rest-api-spec/test (or /rest-api-spec/api) is optional
            String newPath = optionalPathPrefix + "/" + path;
            resource = findResource(newPath, optionalFileSuffix);
            if (resource == null) {
                //if it wasn't on classpath we look outside of the classpath
                Path file = findFile(path, optionalFileSuffix);
                if (!Files.exists(file)) {
                    throw new NoSuchFileException(path);
                }
                return file;
            }
        }

        try {
            return PathUtils.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static URL findResource(String path, String optionalFileSuffix) {
        URL resource = FileUtils.class.getResource(path);
        if (resource == null) {
            //if not found we append the file suffix to the path (as it is optional)
            if (Strings.hasLength(optionalFileSuffix) && !path.endsWith(optionalFileSuffix)) {
                resource = FileUtils.class.getResource(path + optionalFileSuffix);
            }
        }
        return resource;
    }

    private static Path findFile(String path, String optionalFileSuffix) {
        Path file = PathUtils.get(path);
        if (!Files.exists(file)) {
            file = PathUtils.get(path + optionalFileSuffix);
        }
        return file;
    }

    private static void collectFiles(final Path dir, final String fileSuffix, final Map<String, Set<Path>> files) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(fileSuffix)) {
                    String groupName = file.toAbsolutePath().getParent().getFileName().toString();
                    Set<Path> filesSet = files.get(groupName);
                    if (filesSet == null) {
                        filesSet = Sets.newHashSet();
                        files.put(groupName, filesSet);
                    }
                    filesSet.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
