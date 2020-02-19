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

package org.elasticsearch.common.xcontent;

/**
 * Thrown when {@link NamedXContentRegistry} cannot locate a named object to
 * parse for a particular name
 */
public class NamedObjectNotFoundException extends XContentParseException {
    private final Iterable<String> candidates;

    public NamedObjectNotFoundException(XContentLocation location, String message, Iterable<String> candidates) {
        super(location, message);
        this.candidates = candidates;
    }

    /**
     * The possible matches.
     */
    public Iterable<String> getCandidates() {
        return candidates;
    }
}
