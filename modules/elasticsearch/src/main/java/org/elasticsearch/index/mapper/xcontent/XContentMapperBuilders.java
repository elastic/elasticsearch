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

package org.elasticsearch.index.mapper.xcontent;

/**
 * @author kimchy (shay.banon)
 */
public final class XContentMapperBuilders {

    private XContentMapperBuilders() {

    }

    public static XContentDocumentMapper.Builder doc(XContentObjectMapper.Builder objectBuilder) {
        return new XContentDocumentMapper.Builder(objectBuilder);
    }

    public static XContentSourceFieldMapper.Builder source() {
        return new XContentSourceFieldMapper.Builder();
    }

    public static XContentIdFieldMapper.Builder id() {
        return new XContentIdFieldMapper.Builder();
    }

    public static XContentUidFieldMapper.Builder uid() {
        return new XContentUidFieldMapper.Builder();
    }

    public static XContentTypeFieldMapper.Builder type() {
        return new XContentTypeFieldMapper.Builder();
    }

    public static XContentBoostFieldMapper.Builder boost(String name) {
        return new XContentBoostFieldMapper.Builder(name);
    }

    public static XContentAllFieldMapper.Builder all() {
        return new XContentAllFieldMapper.Builder();
    }

    public static XContentMultiFieldMapper.Builder multiField(String name) {
        return new XContentMultiFieldMapper.Builder(name);
    }

    public static XContentObjectMapper.Builder object(String name) {
        return new XContentObjectMapper.Builder(name);
    }

    public static XContentBooleanFieldMapper.Builder booleanField(String name) {
        return new XContentBooleanFieldMapper.Builder(name);
    }

    public static XContentStringFieldMapper.Builder stringField(String name) {
        return new XContentStringFieldMapper.Builder(name);
    }

    public static XContentBinaryFieldMapper.Builder binaryField(String name) {
        return new XContentBinaryFieldMapper.Builder(name);
    }

    public static XContentDateFieldMapper.Builder dateField(String name) {
        return new XContentDateFieldMapper.Builder(name);
    }

    public static XContentShortFieldMapper.Builder shortField(String name) {
        return new XContentShortFieldMapper.Builder(name);
    }

    public static XContentIntegerFieldMapper.Builder integerField(String name) {
        return new XContentIntegerFieldMapper.Builder(name);
    }

    public static XContentLongFieldMapper.Builder longField(String name) {
        return new XContentLongFieldMapper.Builder(name);
    }

    public static XContentFloatFieldMapper.Builder floatField(String name) {
        return new XContentFloatFieldMapper.Builder(name);
    }

    public static XContentDoubleFieldMapper.Builder doubleField(String name) {
        return new XContentDoubleFieldMapper.Builder(name);
    }
}
