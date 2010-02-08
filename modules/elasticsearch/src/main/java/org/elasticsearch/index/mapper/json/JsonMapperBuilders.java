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

package org.elasticsearch.index.mapper.json;

/**
 * @author kimchy (Shay Banon)
 */
public final class JsonMapperBuilders {

    private JsonMapperBuilders() {

    }

    public static JsonDocumentMapper.Builder doc(JsonObjectMapper.Builder objectBuilder) {
        return new JsonDocumentMapper.Builder(objectBuilder);
    }

    public static JsonSourceFieldMapper.Builder source(String name) {
        return new JsonSourceFieldMapper.Builder(name);
    }

    public static JsonIdFieldMapper.Builder id(String name) {
        return new JsonIdFieldMapper.Builder(name);
    }

    public static JsonUidFieldMapper.Builder uid(String name) {
        return new JsonUidFieldMapper.Builder(name);
    }

    public static JsonTypeFieldMapper.Builder type(String name) {
        return new JsonTypeFieldMapper.Builder(name);
    }

    public static JsonBoostFieldMapper.Builder boost(String name) {
        return new JsonBoostFieldMapper.Builder(name);
    }

    public static JsonObjectMapper.Builder object(String name) {
        return new JsonObjectMapper.Builder(name);
    }

    public static JsonBooleanFieldMapper.Builder booleanField(String name) {
        return new JsonBooleanFieldMapper.Builder(name);
    }

    public static JsonStringFieldMapper.Builder stringField(String name) {
        return new JsonStringFieldMapper.Builder(name);
    }

    public static JsonBinaryFieldMapper.Builder binaryField(String name) {
        return new JsonBinaryFieldMapper.Builder(name);
    }

    public static JsonDateFieldMapper.Builder dateField(String name) {
        return new JsonDateFieldMapper.Builder(name);
    }

    public static JsonIntegerFieldMapper.Builder integerField(String name) {
        return new JsonIntegerFieldMapper.Builder(name);
    }

    public static JsonLongFieldMapper.Builder longField(String name) {
        return new JsonLongFieldMapper.Builder(name);
    }

    public static JsonFloatFieldMapper.Builder floatField(String name) {
        return new JsonFloatFieldMapper.Builder(name);
    }

    public static JsonDoubleFieldMapper.Builder doubleField(String name) {
        return new JsonDoubleFieldMapper.Builder(name);
    }
}
