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

package org.elasticsearch.index.field.data.strings;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.field.data.DocFieldData;

/**
 *
 */
public class StringDocFieldData extends DocFieldData<StringFieldData> {

    public StringDocFieldData(StringFieldData fieldData) {
        super(fieldData);
    }

    public String getValue() {
        BytesRef value = fieldData.value(docId);
        if (value == null) {
            return null;
        }
        return value.utf8ToString();
    }

    public String[] getValues() {
        BytesRef[] values = fieldData.values(docId);
        if (values == null) {
            return null;
        }
        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            stringValues[i] = values[i].utf8ToString();
        }
        return stringValues;
    }

    public BytesRef getBytesValue() {
        return fieldData.value(docId);
    }

    public BytesRef[] getBytesValues() {
        return fieldData.values(docId);
    }
}
