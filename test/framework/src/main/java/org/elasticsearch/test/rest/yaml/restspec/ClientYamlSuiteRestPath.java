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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClientYamlSuiteRestPath {
    private final List<PathPart> parts;
    private final List<String> placeholders;

    public ClientYamlSuiteRestPath(List<String> parts) {
        List<PathPart> pathParts = new ArrayList<>(parts.size());
        for (String part : parts) {
            pathParts.add(new PathPart(part, false));
        }
        this.parts = pathParts;
        this.placeholders = Collections.emptyList();
    }

    public ClientYamlSuiteRestPath(String path) {
        String[] pathParts = path.split("/");
        List<String> placeholders = new ArrayList<>();
        List<PathPart> parts = new ArrayList<>();
        for (String pathPart : pathParts) {
            if (pathPart.length() > 0) {
                if (pathPart.startsWith("{")) {
                    if (pathPart.indexOf('}') != pathPart.length() - 1) {
                        throw new IllegalArgumentException("more than one parameter found in the same path part: [" + pathPart + "]");
                    }
                    String placeholder = pathPart.substring(1, pathPart.length() - 1);
                    parts.add(new PathPart(placeholder, true));
                    placeholders.add(placeholder);
                } else {
                    parts.add(new PathPart(pathPart, false));
                }
            }
        }
        this.placeholders = placeholders;
        this.parts = parts;
    }

    public String[] getPathParts() {
        String[] parts = new String[this.parts.size()];
        int i = 0;
        for (PathPart part : this.parts) {
            parts[i++] = part.pathPart;
        }
        return parts;
    }

    public boolean matches(Set<String> params) {
        return placeholders.size() == params.size() && placeholders.containsAll(params);
    }

    public ClientYamlSuiteRestPath replacePlaceholders(Map<String,String> params) {
        List<String> finalPathParts = new ArrayList<>(parts.size());
        for (PathPart pathPart : parts) {
            if (pathPart.isPlaceholder) {
                String value = params.get(pathPart.pathPart);
                if (value == null) {
                    throw new IllegalArgumentException("parameter [" + pathPart.pathPart + "] missing");
                }
                finalPathParts.add(value);
            } else {
                finalPathParts.add(pathPart.pathPart);
            }
        }
        return new ClientYamlSuiteRestPath(finalPathParts);
    }

    private static class PathPart {
        private final boolean isPlaceholder;
        private final String pathPart;

        private PathPart(String pathPart, boolean isPlaceholder) {
            this.isPlaceholder = isPlaceholder;
            this.pathPart = pathPart;
        }
    }
}
