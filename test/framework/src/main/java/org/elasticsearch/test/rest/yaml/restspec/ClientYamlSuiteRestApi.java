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

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Specification of an Elasticsearch endpoint used by the YAML specs to generate REST requests.
 */
public class ClientYamlSuiteRestApi {

    private final String location;
    private final String name;
    private List<String> methods = new ArrayList<>();
    private List<String> paths = new ArrayList<>();
    private Map<String, Boolean> pathParts = new HashMap<>();
    private Map<String, Boolean> params = new HashMap<>();
    private Body body = Body.NOT_SUPPORTED;

    public enum Body {
        NOT_SUPPORTED, OPTIONAL, REQUIRED
    }

    ClientYamlSuiteRestApi(String location, String name) {
        this.location = location;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public List<String> getMethods() {
        return methods;
    }

    /**
     * Returns the supported http methods given the rest parameters provided
     */
    public List<String> getSupportedMethods(Set<String> restParams) {
        //we try to avoid hardcoded mappings but the index api is the exception
        if ("index".equals(name) || "create".equals(name)) {
            List<String> indexMethods = new ArrayList<>();
            for (String method : methods) {
                if (restParams.contains("id")) {
                    //PUT when the id is provided
                    if (HttpPut.METHOD_NAME.equals(method)) {
                        indexMethods.add(method);
                    }
                } else {
                    //POST without id
                    if (HttpPost.METHOD_NAME.equals(method)) {
                        indexMethods.add(method);
                    }
                }
            }
            return indexMethods;
        }

        return methods;
    }

    void addMethod(String method) {
        this.methods.add(method);
    }

    public List<String> getPaths() {
        return paths;
    }

    void addPath(String path) {
        this.paths.add(path);
    }

    /**
     * Gets all path parts supported by the api. For every path part defines if it
     * is required or optional.
     */
    public Map<String, Boolean> getPathParts() {
        return pathParts;
    }

    void addPathPart(String pathPart, boolean required) {
        this.pathParts.put(pathPart, required);
    }

    /**
     * Gets all parameters supported by the api. For every parameter defines if it
     * is required or optional.
     */
    public Map<String, Boolean> getParams() {
        return params;
    }

    void addParam(String param, boolean required) {
        this.params.put(param, required);
    }

    void setBodyOptional() {
        this.body = Body.OPTIONAL;
    }

    void setBodyRequired() {
        this.body = Body.REQUIRED;
    }

    public boolean isBodySupported() {
        return body != Body.NOT_SUPPORTED;
    }

    public boolean isBodyRequired() {
        return body == Body.REQUIRED;
    }

    /**
     * Finds the best matching rest path given the current parameters and replaces
     * placeholders with their corresponding values received as arguments
     */
    public ClientYamlSuiteRestPath[] getFinalPaths(Map<String, String> pathParams) {
        List<ClientYamlSuiteRestPath> matchingRestPaths = findMatchingRestPaths(pathParams.keySet());
        if (matchingRestPaths == null || matchingRestPaths.isEmpty()) {
            throw new IllegalArgumentException("unable to find matching rest path for api [" + name + "] and path params " + pathParams);
        }

        ClientYamlSuiteRestPath[] restPaths = new ClientYamlSuiteRestPath[matchingRestPaths.size()];
        for (int i = 0; i < matchingRestPaths.size(); i++) {
            ClientYamlSuiteRestPath restPath = matchingRestPaths.get(i);
            restPaths[i] = restPath.replacePlaceholders(pathParams);
        }
        return restPaths;
    }

    /**
     * Finds the matching rest paths out of the available ones with the current api (based on REST spec).
     *
     * The best path is the one that has exactly the same number of placeholders to replace
     * (e.g. /{index}/{type}/{id} when the path params are exactly index, type and id).
     */
    private List<ClientYamlSuiteRestPath> findMatchingRestPaths(Set<String> restParams) {

        List<ClientYamlSuiteRestPath> matchingRestPaths = new ArrayList<>();
        ClientYamlSuiteRestPath[] restPaths = buildRestPaths();
        for (ClientYamlSuiteRestPath restPath : restPaths) {
            if (restPath.matches(restParams)) {
                matchingRestPaths.add(restPath);
            }
        }
        return matchingRestPaths;
    }

    private ClientYamlSuiteRestPath[] buildRestPaths() {
        ClientYamlSuiteRestPath[] restPaths = new ClientYamlSuiteRestPath[paths.size()];
        for (int i = 0; i < restPaths.length; i++) {
            restPaths[i] = new ClientYamlSuiteRestPath(paths.get(i));
        }
        return restPaths;
    }
}
