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
import org.elasticsearch.test.rest.client.RestPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents an elasticsearch REST endpoint (api)
 */
public class RestApi {

    private final String location;
    private final String name;
    private List<String> methods = new ArrayList<>();
    private List<String> paths = new ArrayList<>();
    private List<String> pathParts = new ArrayList<>();
    private List<String> params = new ArrayList<>();
    private BODY body = BODY.NOT_SUPPORTED;

    public enum BODY {
        NOT_SUPPORTED, OPTIONAL, REQUIRED
    }

    RestApi(String location, String name) {
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
    public RestPath[] getFinalPaths(Map<String, String> pathParams) {
        List<RestPath> matchingRestPaths = findMatchingRestPaths(pathParams.keySet());
        if (matchingRestPaths == null || matchingRestPaths.isEmpty()) {
            throw new IllegalArgumentException("unable to find matching rest path for api [" + name + "] and path params " + pathParams);
        }

        RestPath[] restPaths = new RestPath[matchingRestPaths.size()];
        for (int i = 0; i < matchingRestPaths.size(); i++) {
            RestPath restPath = matchingRestPaths.get(i);
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
    private List<RestPath> findMatchingRestPaths(Set<String> restParams) {

        List<RestPath> matchingRestPaths = new ArrayList<>();
        RestPath[] restPaths = buildRestPaths();
        for (RestPath restPath : restPaths) {
            if (restPath.matches(restParams)) {
                matchingRestPaths.add(restPath);
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
}
