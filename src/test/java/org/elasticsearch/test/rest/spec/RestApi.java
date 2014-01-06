/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.spec;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.lucene.util.PriorityQueue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents an elasticsearch REST endpoint (api)
 */
public class RestApi {

    private static final String ALL = "_all";

    private final String name;
    private List<String> methods = Lists.newArrayList();
    private List<String> paths = Lists.newArrayList();
    private List<String> pathParts = Lists.newArrayList();

    RestApi(String name) {
        this.name = name;
    }

    RestApi(RestApi restApi, String name, String... methods) {
        this.name = name;
        this.methods = Arrays.asList(methods);
        paths.addAll(restApi.getPaths());
        pathParts.addAll(restApi.getPathParts());
    }

    RestApi(RestApi restApi, List<String> paths) {
        this.name = restApi.getName();
        this.methods = restApi.getMethods();
        this.paths.addAll(paths);
        pathParts.addAll(restApi.getPathParts());
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
            List<String> indexMethods = Lists.newArrayList();
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

    /**
     * Finds the best matching rest path given the current parameters and replaces
     * placeholders with their corresponding values received as arguments
     */
    public String getFinalPath(Map<String, String> pathParams) {
        RestPath matchingRestPath = findMatchingRestPath(pathParams.keySet());
        String path = matchingRestPath.path;
        for (Map.Entry<String, String> paramEntry : matchingRestPath.params.entrySet()) {
            //replace path placeholders with actual values
            String value = pathParams.get(paramEntry.getValue());
            if (value == null) {
                //there might be additional placeholder to replace, not available as input params
                //it can only be {index} or {type} to be replaced with _all
                if (paramEntry.getValue().equals("index") || paramEntry.getValue().equals("type")) {
                    value = ALL;
                } else {
                    throw new IllegalArgumentException("path [" + path + "] contains placeholders that weren't replaced with proper values");
                }
            }
            path = path.replace(paramEntry.getKey(), value);
        }
        return path;
    }

    /**
     * Finds the best matching rest path out of the available ones with the current api (based on REST spec).
     *
     * The best path is the one that has exactly the same number of placeholders to replace
     * (e.g. /{index}/{type}/{id} when the params are exactly index, type and id).
     * Otherwise there might be additional placeholders, thus we use the path with the least additional placeholders.
     * (e.g. get with only index and id as parameters, the closest (and only) path contains {type} too, which becomes _all)
     */
    private RestPath findMatchingRestPath(Set<String> restParams) {

        RestPath[] restPaths = buildRestPaths();

        //We need to find the path that has exactly the placeholders corresponding to our params
        //If there's no exact match we fallback to the closest one (with as less additional placeholders as possible)
        //The fallback is needed for:
        //1) get, get_source and exists with only index and id => /{index}/_all/{id} (
        //2) search with only type => /_all/{type/_search
        PriorityQueue<RestPath> restPathQueue = new PriorityQueue<RestPath>(1) {
            @Override
            protected boolean lessThan(RestPath a, RestPath b) {
                return a.params.size() >= b.params.size();
            }
        };
        for (RestPath restPath : restPaths) {
            if (restPath.params.values().containsAll(restParams)) {
                restPathQueue.insertWithOverflow(restPath);
            }
        }

        if (restPathQueue.size() > 0) {
            return restPathQueue.top();
        }

        throw new IllegalArgumentException("unable to find best path for api [" + name + "] and params " + restParams);
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
        final Map<String, String> params;

        RestPath(String path) {
            this.path = path;
            this.params = extractParams(path);
        }

        private static Map<String,String> extractParams(String input) {
            Map<String, String> params = Maps.newHashMap();
            Matcher matcher = PLACEHOLDERS_PATTERN.matcher(input);
            while (matcher.find()) {
                //key is e.g. {index}
                String key = input.substring(matcher.start(), matcher.end());
                if (matcher.groupCount() != 2) {
                    throw new IllegalArgumentException("no lookup key found for param [" + key + "]");
                }
                //to be replaced with current value found with key e.g. index
                String value = matcher.group(2);
                params.put(key, value);
            }
            return params;
        }
    }
}
