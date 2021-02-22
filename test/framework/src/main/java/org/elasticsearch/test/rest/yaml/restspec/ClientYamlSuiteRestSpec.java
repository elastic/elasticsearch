/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ClasspathUtils;

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

/**
 * Holds the specification used to turn {@code do} actions in the YAML suite into REST api calls.
 */
public class ClientYamlSuiteRestSpec {
    private final Set<String> globalParameters = new HashSet<>();
    private final Map<String, ClientYamlSuiteRestApi> restApiMap = new HashMap<>();

    ClientYamlSuiteRestSpec() {}

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
        Path[] dirs = ClasspathUtils.findFilePaths(ClientYamlSuiteRestSpec.class.getClassLoader(), classpathPrefix);
        ClientYamlSuiteRestSpec restSpec = new ClientYamlSuiteRestSpec();
        ClientYamlSuiteRestApiParser restApiParser = new ClientYamlSuiteRestApiParser();
        for (Path dir : dirs) {
            try (Stream<Path> stream = Files.walk(dir)) {
                stream.forEach(item -> {
                    if (item.toString().endsWith(".json")) {
                        parseSpecFile(restApiParser, item, restSpec);
                    }
                });
            }
        }
        return restSpec;
    }

    private static void parseSpecFile(ClientYamlSuiteRestApiParser restApiParser, Path jsonFile, ClientYamlSuiteRestSpec restSpec) {
        try (InputStream stream = Files.newInputStream(jsonFile)) {
            try (XContentParser parser =
                     JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                String filename = jsonFile.getFileName().toString();
                if (filename.equals("_common.json")) {
                    parseCommonSpec(parser, restSpec);
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

    static void parseCommonSpec(XContentParser parser, ClientYamlSuiteRestSpec restSpec) throws IOException {
        String currentFieldName = null;
        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
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
                } else {
                    parser.skipChildren();
                }
            }
        }

    }
}
