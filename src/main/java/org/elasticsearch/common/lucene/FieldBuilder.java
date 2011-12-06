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

package org.elasticsearch.common.lucene;

import org.apache.lucene.document.Field;

/**
 *
 */
public class FieldBuilder {

    private final Field field;

    FieldBuilder(String name, String value, Field.Store store, Field.Index index) {
        field = new Field(name, value, store, index);
    }

    FieldBuilder(String name, String value, Field.Store store, Field.Index index, Field.TermVector termVector) {
        field = new Field(name, value, store, index, termVector);
    }

    FieldBuilder(String name, byte[] value, Field.Store store) {
        field = new Field(name, value, store);
    }

    FieldBuilder(String name, byte[] value, int offset, int length, Field.Store store) {
        field = new Field(name, value, offset, length, store);
    }

    public FieldBuilder boost(float boost) {
        field.setBoost(boost);
        return this;
    }

    public FieldBuilder omitNorms(boolean omitNorms) {
        field.setOmitNorms(omitNorms);
        return this;
    }

    public FieldBuilder omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
        field.setOmitTermFreqAndPositions(omitTermFreqAndPositions);
        return this;
    }

    public Field build() {
        return field;
    }
}
