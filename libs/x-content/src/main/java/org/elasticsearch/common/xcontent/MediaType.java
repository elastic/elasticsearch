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

/**
 * Abstracts a <a href="http://en.wikipedia.org/wiki/Internet_media_type">Media Type</a> and a format parameter.
 * Media types are used as values on Content-Type and Accept headers
 * format is an URL parameter, specifies response media type.
 */
public interface MediaType {
    /**
     * Returns a type part of a MediaType
     * i.e. application for application/json
     */
    String type();

    /**
     * Returns a subtype part of a MediaType.
     * i.e. json for application/json
     */
    String subtype();

    /**
     * Returns a corresponding format for a MediaType. i.e. json for application/json media type
     * Can differ from the MediaType's subtype i.e plain/text has a subtype of text but format is txt
     */
    String format();

    /**
     * returns a string representation of a media type.
     */
    default String typeWithSubtype(){
        return type() + "/" + subtype();
    }
}
