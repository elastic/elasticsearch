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

package org.elasticsearch.common.lucene.document;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.FieldInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SingleFieldVisitor extends BaseFieldVisitor {

    private String name;
    private List<String> values;

    public SingleFieldVisitor() {
    }

    public SingleFieldVisitor(String name) {
        this.name = name;
    }

    public void name(String name) {
        this.name = name;
    }

    @Override
    public Document createDocument() {
        Document document = new Document();
        for (String value : values) {
            document.add(new StoredField(name, value));
        }
        return document;
    }

    public String value() {
        return values.get(0);
    }

    public List<String> values() {
        return values;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (name.equals(fieldInfo.name)) {
            return Status.YES;
        }

        return values != null ? Status.STOP : Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        if (fieldInfo.name.equals(name)) {
            if (values == null) {
                values = new ArrayList<String>();
            }
            values.add(value);
        }
    }
}
