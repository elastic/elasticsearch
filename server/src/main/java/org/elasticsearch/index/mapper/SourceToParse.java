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

import java.util.Objects;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

public class SourceToParse {

    public static SourceToParse source(String index, String type, String id, BytesReference source,
                                       XContentType contentType) {
        return new SourceToParse(index, type, id, source, contentType);
    }

    private final BytesReference source;

    private final String index;

    private final String type;

    private final String id;

    private String routing;

    private String parentId;

    private XContentType xContentType;

    private SourceToParse(String index, String type, String id, BytesReference source, XContentType xContentType) {
        this.index = Objects.requireNonNull(index);
        this.type = Objects.requireNonNull(type);
        this.id = Objects.requireNonNull(id);
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        this.source = new BytesArray(Objects.requireNonNull(source).toBytesRef());
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    public BytesReference source() {
        return this.source;
    }

    public String index() {
        return this.index;
    }

    public String type() {
        return this.type;
    }

    public String id() {
        return this.id;
    }

    public String parent() {
        return this.parentId;
    }

    public SourceToParse parent(String parentId) {
        this.parentId = parentId;
        return this;
    }

    public String routing() {
        return this.routing;
    }

    public XContentType getXContentType() {
        return this.xContentType;
    }

    public SourceToParse routing(String routing) {
        this.routing = routing;
        return this;
    }

    public enum Origin {
        PRIMARY,
        REPLICA
    }
}
