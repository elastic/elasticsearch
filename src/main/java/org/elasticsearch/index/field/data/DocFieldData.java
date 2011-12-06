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

package org.elasticsearch.index.field.data;

/**
 *
 */
public abstract class DocFieldData<T extends FieldData> {

    protected final T fieldData;

    protected int docId;

    protected DocFieldData(T fieldData) {
        this.fieldData = fieldData;
    }

    void setDocId(int docId) {
        this.docId = docId;
    }

    public String getFieldName() {
        return fieldData.fieldName();
    }

    public boolean isEmpty() {
        return !fieldData.hasValue(docId);
    }

    public String stringValue() {
        return fieldData.stringValue(docId);
    }

    public String getStringValue() {
        return stringValue();
    }

    public FieldDataType getType() {
        return fieldData.type();
    }

    public boolean isMultiValued() {
        return fieldData.multiValued();
    }
}
