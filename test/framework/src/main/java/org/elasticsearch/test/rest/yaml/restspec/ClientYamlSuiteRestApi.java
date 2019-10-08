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

import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Specification of an Elasticsearch endpoint used by the YAML specs to generate REST requests.
 */
public class ClientYamlSuiteRestApi {

    private final String location;
    private final String name;
    private Set<Path>  paths = new LinkedHashSet<>();
    private Map<String, Boolean> params = new HashMap<>();
    private Body body = Body.NOT_SUPPORTED;
    private Stability stability;

    public enum Stability {
        EXPERIMENTAL, BETA, STABLE
    }

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

    void addPath(String path, String[] methods, Set<String> parts) {
        Objects.requireNonNull(path, name + " API: path must not be null");
        Objects.requireNonNull(methods, name + " API: methods must not be null");
        if (methods.length == 0) {
            throw new IllegalArgumentException(name + " API: methods is empty, at least one method is required");
        }
        Objects.requireNonNull(parts, name + " API: parts must not be null");
        for (String part : parts) {
            if (path.contains("{" + part + "}") == false) {
                throw new IllegalArgumentException(name + " API: part [" + part + "] not contained in path [" + path + "]");
            }
        }
        boolean add = this.paths.add(new Path(path, methods, parts));
        if (add == false) {
            throw new IllegalArgumentException(name + " API: found duplicate path [" + path + "]");
        }
    }

    public Collection<Path> getPaths() {
        return paths;
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

    public void setStability(String stability) {
        this.stability = Stability.valueOf(stability.toUpperCase(Locale.ROOT));
    }

    public Stability getStability() { return this.stability; }

    /**
     * Returns the best matching paths based on the provided parameters, which may include either path parts or query_string parameters.
     * The best path is the one that has exactly the same number of placeholders to replace
     * (e.g. /{index}/{type}/{id} when the path params are exactly index, type and id).
     * It returns a list instead of a single path as there are cases where there is more than one best matching path:
     * - /{index}/_alias/{name}, /{index}/_aliases/{name}
     * - /{index}/{type}/_mapping, /{index}/{type}/_mappings, /{index}/_mappings/{type}, /{index}/_mapping/{type}
     */
    public List<ClientYamlSuiteRestApi.Path> getBestMatchingPaths(Set<String> params) {
        PriorityQueue<Tuple<Integer, Path>> queue = new PriorityQueue<>(Comparator.comparing(Tuple::v1, (a, b) -> Integer.compare(b, a)));
        for (ClientYamlSuiteRestApi.Path path : paths) {
            int matches = 0;
            for (String actualParameter : params) {
                if (path.getParts().contains(actualParameter)) {
                    matches++;
                }
            }
            if (matches == path.parts.size()) {
                queue.add(Tuple.tuple(matches, path));
            }
        }
        if (queue.isEmpty()) {
            throw new IllegalStateException("Unable to find a matching path for api [" + name + "]" + params);
        }
        List<Path> paths = new ArrayList<>();
        Tuple<Integer, Path> poll = queue.poll();
        int maxMatches = poll.v1();
        do {
            paths.add(poll.v2());
            poll = queue.poll();
        } while (poll != null && poll.v1() == maxMatches);

        return paths;
    }

    public static class Path {
        private final String path;
        private final String[] methods;
        private final Set<String> parts;

        private Path(String path, String[] methods, Set<String> parts) {
            this.path = path;
            this.methods = methods;
            this.parts = parts;
        }

        public String getPath() {
            return path;
        }

        public String[] getMethods() {
            return methods;
        }

        public Set<String> getParts() {
            return parts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Path path = (Path) o;
            return this.path.equals(path.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path);
        }
    }
}
