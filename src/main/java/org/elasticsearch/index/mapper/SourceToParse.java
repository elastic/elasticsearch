/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.xcontent.XContentParser;

/**
 *
 */
public class SourceToParse {

    public static SourceToParse source(byte[] source) {
        return new SourceToParse(source);
    }

    public static SourceToParse source(byte[] source, int offset, int length) {
        return new SourceToParse(source, offset, length);
    }

    public static SourceToParse source(XContentParser parser) {
        return new SourceToParse(parser);
    }

    private final byte[] source;
    private final int sourceOffset;
    private final int sourceLength;

    private final XContentParser parser;

    private boolean flyweight = false;

    private String type;

    private String id;

    private String routing;

    private String parentId;

    private long timestamp;

    private long ttl;

    public SourceToParse(XContentParser parser) {
        this.parser = parser;
        this.source = null;
        this.sourceOffset = 0;
        this.sourceLength = 0;
    }

    public SourceToParse(byte[] source) {
        this.source = source;
        this.sourceOffset = 0;
        this.sourceLength = source.length;
        this.parser = null;
    }

    public SourceToParse(byte[] source, int offset, int length) {
        this.source = source;
        this.sourceOffset = offset;
        this.sourceLength = length;
        this.parser = null;
    }

    public XContentParser parser() {
        return this.parser;
    }

    public byte[] source() {
        return this.source;
    }

    public int sourceOffset() {
        return this.sourceOffset;
    }

    public int sourceLength() {
        return this.sourceLength;
    }

    public String type() {
        return this.type;
    }

    public SourceToParse type(String type) {
        this.type = type;
        return this;
    }

    public SourceToParse flyweight(boolean flyweight) {
        this.flyweight = flyweight;
        return this;
    }

    public boolean flyweight() {
        return this.flyweight;
    }

    public String id() {
        return this.id;
    }

    public SourceToParse id(String id) {
        this.id = id;
        return this;
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

    public SourceToParse routing(String routing) {
        this.routing = routing;
        return this;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public SourceToParse timestamp(String timestamp) {
        this.timestamp = Long.parseLong(timestamp);
        return this;
    }

    public SourceToParse timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public long ttl() {
        return this.ttl;
    }

    public SourceToParse ttl(long ttl) {
        this.ttl = ttl;
        return this;
    }
}
