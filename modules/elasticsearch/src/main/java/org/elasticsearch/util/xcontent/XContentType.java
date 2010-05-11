/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.xcontent;

/**
 * The content type of {@link org.elasticsearch.util.xcontent.XContent}.
 *
 * @author kimchy (shay.banon)
 */
public enum XContentType {

    /**
     * A JSON based content type.
     */
    JSON(0),
    /**
     * An optimized binary form of JSON.
     */
    XSON(1);

    public static XContentType fromRestContentType(String contentType) {
        if (contentType == null) {
            return null;
        }
        if ("application/json".equals(contentType) || "json".equalsIgnoreCase(contentType)) {
            return JSON;
        }

        if ("application/xson".equals(contentType) || "xson".equalsIgnoreCase(contentType)) {
            return XSON;
        }

        return null;
    }

    private int index;

    XContentType(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }
}
