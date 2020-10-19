/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Abstracts a <a href="http://en.wikipedia.org/wiki/Internet_media_type">Media Type</a> and a format parameter.
 * Media types are used as values on Content-Type and Accept headers
 */
public interface MediaType {
    /**
     * The headers that can represent this MediaType, does not include the optional parameters
     * i.e. "application/json", "application/vnd.elasticsearch+json"
     * MUST be represented in all lowercase.
     */
    Set<String> mimeTypes();

    /**
     * The valid parameters associated with this Media type. An empty collection should be returned to support any parameters.
     */
    default Map<String, String> params() {
        return Collections.emptyMap();
    }

    /**
     * The representation for the canonical string representation of this MediaType, i.e. 'application/json'
     */
    default String canonical(){
        return mimeTypes().iterator().next();
    }

    /**
     * A common short name to use to represent this MediaType. This can be used in HTTP params or log messages.
     * Can differ from the MediaType's subtype i.e plain/text has a subtype of text but can be txt
     */
    String shortName();
}
