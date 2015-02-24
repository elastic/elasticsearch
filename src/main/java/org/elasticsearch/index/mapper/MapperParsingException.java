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

package org.elasticsearch.index.mapper;

import org.elasticsearch.rest.RestStatus;

/**
 *
 */
public class MapperParsingException extends MapperException {

    public MapperParsingException(String message) {
        super(message);
        mappingsModified = false;
    }

    public boolean isMappingsModified() {
        return mappingsModified;
    }

    private boolean mappingsModified = false;

    public MapperParsingException(String message, boolean mappingsModified) {
        super(message);
        this.mappingsModified = mappingsModified;
    }

    public MapperParsingException(String message, Throwable cause, boolean mappingsModified) {
        super(message, cause);
        this.mappingsModified = mappingsModified;
    }

    public MapperParsingException(String message, Throwable cause) {
        super(message, cause);
        this.mappingsModified = false;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}