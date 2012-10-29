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

package org.elasticsearch.index.mapper.selector;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.document.BaseFieldVisitor;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;

/**
 * An optimized field selector that loads just the uid and the source.
 */
public class UidAndSourceFieldVisitor extends BaseFieldVisitor {

    private String uid;
    private BytesRef source;

    @Override
    public Document createDocument() {
        Document document = new Document();
        document.add(new StoredField("uid", uid));
        document.add(new StoredField("_source", source));
        return document;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        } else if (UidFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }

        return uid != null && source != null ? Status.STOP : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        source = new BytesRef(value);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        uid = value;
    }

    @Override
    public String toString() {
        return "uid_and_source";
    }
}
