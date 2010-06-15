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

package org.elasticsearch.common.xcontent.xson;

/**
 * @author kimchy (shay.banon)
 */
public enum XsonType {

    START_ARRAY((byte) 0x01),
    END_ARRAY((byte) 0x02),
    START_OBJECT((byte) 0x03),
    END_OBJECT((byte) 0x04),
    FIELD_NAME((byte) 0x05),
    VALUE_STRING((byte) 0x06),
    VALUE_BINARY((byte) 0x07),
    VALUE_INTEGER((byte) 0x08),
    VALUE_LONG((byte) 0x09),
    VALUE_FLOAT((byte) 0x0A),
    VALUE_DOUBLE((byte) 0x0B),
    VALUE_BOOLEAN((byte) 0x0C),
    VALUE_NULL((byte) 0x0D),;

    public static final int HEADER = 0x00;

    private final byte code;

    XsonType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}
