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

package org.elasticsearch.index.mapper;

/**
 * @author kimchy (Shay Banon)
 */
public final class Uid {

    public static final char DELIMITER = '#';

    private final String type;

    private final String id;

    public Uid(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public static Uid createUid(String uid) {
        int delimiterIndex = uid.lastIndexOf(DELIMITER);
        return new Uid(uid.substring(0, delimiterIndex), uid.substring(delimiterIndex + 1));
    }

    public static String createUid(String type, String id) {
        return createUid(new StringBuilder(), type, id);
    }

    public static String createUid(StringBuilder sb, String type, String id) {
        return sb.append(type).append(DELIMITER).append(id).toString();
    }
}
