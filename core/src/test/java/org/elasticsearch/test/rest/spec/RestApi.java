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
package org.elasticsearch.test.rest.spec;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents an elasticsearch REST endpoint (api)
 */
public class RestApi {

    private final String name;
    private List<String> methods = new ArrayList<>();
    private List<String> paths = new ArrayList<>();
    private List<String> pathParts = new ArrayList<>();
    private List<String> params = new ArrayList<>();
    private BODY body = BODY.NOT_SUPPORTED;

    public static enum BODY {
        NOT_SUPPORTED, OPTIONAL, REQUIRED
    }

    RestApi(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
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

    public List<String> getPathParts() {
        return pathParts;
    }

    void addPathPart(String pathPart) {
        this.pathParts.add(pathPart);
    }

    public List<String> getParams() {
        return params;
    }

    void addParam(String param) {
        this.params.add(param);
    }

    void setBodyOptional() {
        this.body = BODY.OPTIONAL;
    }

    void setBodyRequired() {
        this.body = BODY.REQUIRED;
    }

    public boolean isBodySupported() {
        return body != BODY.NOT_SUPPORTED;
    }

    public boolean isBodyRequired() {
        return body == BODY.REQUIRED;
    }

    /**
     * Finds the best matching rest path given the current parameters and replaces
     * placeholders with their corresponding values received as arguments
     */
    public String[] getFinalPaths(Map<String, String> pathParams) {

        List<RestPath> matchingRestPaths = findMatchingRestPaths(pathParams.keySet());
        if (matchingRestPaths == null || matchingRestPaths.isEmpty()) {
            throw new IllegalArgumentException("unable to find matching rest path for api [" + name + "] and path params " + pathParams);
        }

        String[] paths = new String[matchingRestPaths.size()];
        for (int i = 0; i < matchingRestPaths.size(); i++) {
            RestPath restPath = matchingRestPaths.get(i);
            String path = restPath.path;
            for (Map.Entry<String, String> paramEntry : restPath.parts.entrySet()) {
                // replace path placeholders with actual values
                String value = pathParams.get(paramEntry.getValue());
                if (value == null) {
                    throw new IllegalArgumentException("parameter [" + paramEntry.getValue() + "] missing");
                }
                path = path.replace(paramEntry.getKey(), value);
            }
            paths[i] = path;
        }
        return paths;
    }

    /**
     * Finds the matching rest paths out of the available ones with the current api (based on REST spec).
     *
     * The best path is the one that has exactly the same number of placeholders to replace
     * (e.g. /{index}/{type}/{id} when the path params are exactly index, type and id).
     */
    private List<RestPath> findMatchingRestPaths(Set<String> restParams) {

        List<RestPath> matchingRestPaths = new ArrayList<>();
        RestPath[] restPaths = buildRestPaths();

        for (RestPath restPath : restPaths) {
            if (restPath.parts.size() == restParams.size()) {
                if (restPath.parts.values().containsAll(restParams)) {
                    matchingRestPaths.add(restPath);
                }
            }
        }

        return matchingRestPaths;
    }

    private RestPath[] buildRestPaths() {
        RestPath[] restPaths = new RestPath[paths.size()];
        for (int i = 0; i < restPaths.length; i++) {
            restPaths[i] = new RestPath(paths.get(i));
        }
        return restPaths;
    }

    private static class RestPath {
        private static final Pattern PLACEHOLDERS_PATTERN = Pattern.compile("(\\{(.*?)})");

        final String path;
        //contains param to replace (e.g. {index}) and param key to use for lookup in the current values map (e.g. index)
        final Map<String, String> parts;

        RestPath(String path) {
            this.path = path;
            this.parts = extractParts(path);
        }

        private static Map<String,String> extractParts(String input) {
            Map<String, String> parts = new HashMap<>();
            Matcher matcher = PLACEHOLDERS_PATTERN.matcher(input);
            while (matcher.find()) {
                //key is e.g. {index}
                String key = input.substring(matcher.start(), matcher.end());
                if (matcher.groupCount() != 2) {
                    throw new IllegalArgumentException("no lookup key found for param [" + key + "]");
                }
                //to be replaced with current value found with key e.g. index
                String value = matcher.group(2);
                parts.put(key, value);
            }
            return parts;
        }
    }
}
