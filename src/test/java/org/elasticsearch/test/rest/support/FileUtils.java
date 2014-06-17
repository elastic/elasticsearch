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

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URL;
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
    public static Set<File> findJsonSpec(String optionalPathPrefix, String path) throws FileNotFoundException {
        File dir = resolveFile(optionalPathPrefix, path, null);

        if (!dir.isDirectory()) {
            throw new FileNotFoundException("file [" + path + "] is not a directory");
        }

        File[] jsonFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(JSON_SUFFIX);
            }
        });

        if (jsonFiles == null || jsonFiles.length == 0) {
            throw new FileNotFoundException("no json files found within [" + path + "]");
        }

        return Sets.newHashSet(jsonFiles);
    }

    /**
     * Returns the yaml files found within the paths provided.
     * Each input path can either be a single file (the .yaml suffix is optional) or a directory.
     * Each path is looked up in the classpath first, then outside of it if not found yet.
     */
    public static Map<String, Set<File>> findYamlSuites(final String optionalPathPrefix, final String... paths) throws FileNotFoundException {
        Map<String, Set<File>> yamlSuites = Maps.newHashMap();
        for (String path : paths) {
            collectFiles(resolveFile(optionalPathPrefix, path, YAML_SUFFIX), YAML_SUFFIX, yamlSuites);
        }
        return yamlSuites;
    }

    private static File resolveFile(String optionalPathPrefix, String path, String optionalFileSuffix) throws FileNotFoundException {
        //try within classpath with and without file suffix (as it could be a single test suite)
        URL resource = findResource(path, optionalFileSuffix);
        if (resource == null) {
            //try within classpath with optional prefix: /rest-api-spec/test (or /rest-api-spec/api) is optional
            String newPath = optionalPathPrefix + "/" + path;
            resource = findResource(newPath, optionalFileSuffix);
            if (resource == null) {
                //if it wasn't on classpath we look outside ouf the classpath
                File file = findFile(path, optionalFileSuffix);
                if (!file.exists()) {
                    throw new FileNotFoundException("file [" + path + "] doesn't exist");
                }
                return file;
            }
        }

        return new File(URI.create(resource.toString()));
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

    private static File findFile(String path, String optionalFileSuffix) {
        File file = new File(path);
        if (!file.exists()) {
            file = new File(path + optionalFileSuffix);
        }
        return file;
    }

    private static void collectFiles(final File file, final String fileSuffix, final Map<String, Set<File>> files) {
        if (file.isFile()) {
            String groupName = file.getParentFile().getName();
            Set<File> filesSet = files.get(groupName);
            if (filesSet == null) {
                filesSet = Sets.newHashSet();
                files.put(groupName, filesSet);
            }
            filesSet.add(file);
        } else if (file.isDirectory()) {
            walkDir(file, fileSuffix, files);
        }
    }

    private static void walkDir(final File dir, final String fileSuffix, final Map<String, Set<File>> files) {
        File[] children = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory() || pathname.getName().endsWith(fileSuffix);
            }
        });

        for (File file : children) {
            collectFiles(file, fileSuffix, files);
        }
    }
}
