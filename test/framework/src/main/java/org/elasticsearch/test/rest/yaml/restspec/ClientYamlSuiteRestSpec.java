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
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Holds the specification used to turn {@code do} actions in the YAML suite into REST api calls.
 */
public class ClientYamlSuiteRestSpec {
    Map<String, ClientYamlSuiteRestApi> restApiMap = new HashMap<>();

    private ClientYamlSuiteRestSpec() {}

    private void addApi(ClientYamlSuiteRestApi restApi) {
        ClientYamlSuiteRestApi previous = restApiMap.putIfAbsent(restApi.getName(), restApi);
        if (previous != null) {
            throw new IllegalArgumentException("cannot register api [" + restApi.getName() + "] found in [" + restApi.getLocation() + "]. "
                    + "api with same name was already found in [" + previous.getLocation() + "]");
        }
    }

    public ClientYamlSuiteRestApi getApi(String api) {
        return restApiMap.get(api);
    }

    public Collection<ClientYamlSuiteRestApi> getApis() {
        return restApiMap.values();
    }

    /**
     * Parses the complete set of REST spec available under the provided directory
     */
    public static ClientYamlSuiteRestSpec load(String classpathPrefix) throws Exception {
        Path dir = PathUtils.get(ClientYamlSuiteRestSpec.class.getResource(classpathPrefix).toURI());
        ClientYamlSuiteRestSpec restSpec = new ClientYamlSuiteRestSpec();
        ClientYamlSuiteRestApiParser restApiParser = new ClientYamlSuiteRestApiParser();
        try (Stream<Path> stream = Files.walk(dir)) {
            stream.forEach(item -> {
                if (item.toString().endsWith(".json")) {
                    restSpec.addApi(parseSpecFile(restApiParser, item));
                }
            });
        }
        return restSpec;
    }

    private static ClientYamlSuiteRestApi parseSpecFile(ClientYamlSuiteRestApiParser restApiParser, Path jsonFile) {
        try (InputStream stream = Files.newInputStream(jsonFile)) {
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(stream)) {
                ClientYamlSuiteRestApi restApi = restApiParser.parse(jsonFile.toString(), parser);
                String filename = jsonFile.getFileName().toString();
                String expectedApiName = filename.substring(0, filename.lastIndexOf('.'));
                if (restApi.getName().equals(expectedApiName) == false) {
                    throw new IllegalArgumentException("found api [" + restApi.getName() + "] in [" + jsonFile.toString() + "]. " +
                        "Each api is expected to have the same name as the file that defines it.");
                }
                return restApi;
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Can't parse rest spec file: [" + jsonFile + "]", ex);
        }
    }
}
