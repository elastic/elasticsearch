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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

/**
 * Holds the specification used to turn {@code do} actions in the YAML suite into REST api calls.
 */
public class ClientYamlSuiteRestSpec {
    private final Set<String> globalParameters = new HashSet<>();
    private final Map<String, ClientYamlSuiteRestApi> restApiMap = new HashMap<>();

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
     * Returns whether the provided parameter is one of those parameters that are supported by all Elasticsearch api
     */
    public boolean isGlobalParameter(String param) {
        return globalParameters.contains(param);
    }

    /**
     * Returns whether the provided parameter is one of those parameters that are supported by the Elasticsearch language clients, meaning
     * that they influence the client behaviour and don't get sent to Elasticsearch
     */
    public boolean isClientParameter(String name) {
        return "ignore".equals(name);
    }

    /**
     * Parses the complete set of REST spec available under the provided directories
     */
    public static ClientYamlSuiteRestSpec load(String classpathPrefix) throws Exception {
        Path dir = PathUtils.get(ClientYamlSuiteRestSpec.class.getResource(classpathPrefix).toURI());
        ClientYamlSuiteRestSpec restSpec = new ClientYamlSuiteRestSpec();
        ClientYamlSuiteRestApiParser restApiParser = new ClientYamlSuiteRestApiParser();
        try (Stream<Path> stream = Files.walk(dir)) {
            stream.forEach(item -> {
                if (item.toString().endsWith(".json")) {
                    parseSpecFile(restApiParser, item, restSpec);
                }
            });
        }
        return restSpec;
    }

    private static void parseSpecFile(ClientYamlSuiteRestApiParser restApiParser, Path jsonFile, ClientYamlSuiteRestSpec restSpec) {
        try (InputStream stream = Files.newInputStream(jsonFile)) {
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, stream)) {
                String filename = jsonFile.getFileName().toString();
                if (filename.equals("_common.json")) {
                    String currentFieldName = null;
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT
                            && "params".equals(currentFieldName)) {
                            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                                String param = parser.currentName();
                                if (restSpec.globalParameters.contains(param)) {
                                    throw new IllegalArgumentException("Found duplicate global param [" + param + "]");
                                }
                                restSpec.globalParameters.add(param);
                                parser.nextToken();
                                if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                                    throw new IllegalArgumentException("Expected params field in rest api definition to " +
                                        "contain an object");
                                }
                                parser.skipChildren();
                            }
                        }
                    }
                } else {
                    ClientYamlSuiteRestApi restApi = restApiParser.parse(jsonFile.toString(), parser);
                    String expectedApiName = filename.substring(0, filename.lastIndexOf('.'));
                    if (restApi.getName().equals(expectedApiName) == false) {
                        throw new IllegalArgumentException("found api [" + restApi.getName() + "] in [" + jsonFile.toString() + "]. " +
                            "Each api is expected to have the same name as the file that defines it.");
                    }
                    restSpec.addApi(restApi);
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Can't parse rest spec file: [" + jsonFile + "]", ex);
        }
    }
}
