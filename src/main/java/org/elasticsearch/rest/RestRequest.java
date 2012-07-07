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

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;

import java.util.Map;

/**
 *
 */
public interface RestRequest extends ToXContent.Params {

    enum Method {
        GET, POST, PUT, DELETE, OPTIONS, HEAD
    }

    Method method();

    /**
     * The uri of the rest request, with the query string.
     */
    String uri();

    /**
     * The non decoded, raw path provided.
     */
    String rawPath();

    /**
     * The path part of the URI (without the query string), decoded.
     */
    String path();

    boolean hasContent();

    /**
     * Is the byte array content safe or unsafe for usage on other threads
     */
    boolean contentUnsafe();

    BytesReference content();

    String header(String name);

    boolean hasParam(String key);

    String param(String key);

    String[] paramAsStringArray(String key, String[] defaultValue);

    float paramAsFloat(String key, float defaultValue);

    int paramAsInt(String key, int defaultValue);

    long paramAsLong(String key, long defaultValue);

    boolean paramAsBoolean(String key, boolean defaultValue);

    Boolean paramAsBooleanOptional(String key, Boolean defaultValue);

    TimeValue paramAsTime(String key, TimeValue defaultValue);

    ByteSizeValue paramAsSize(String key, ByteSizeValue defaultValue);

    Map<String, String> params();
}
