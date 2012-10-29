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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.document.BaseFieldVisitor;

import java.io.IOException;

/**
 * An optimized field selector that loads just the _source
 */
public class SourceFieldVisitor extends BaseFieldVisitor {

    public static final SourceFieldVisitor INSTANCE = new SourceFieldVisitor();
    private static ThreadLocal<BytesRef> loadingContext = new ThreadLocal<BytesRef>();

    private SourceFieldVisitor() {
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }
        return loadingContext.get() != null ? Status.STOP : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        loadingContext.set(new BytesRef(value));
    }

    @Override
    public void reset() {
        loadingContext.remove();
    }

    @Override
    public Document createDocument() {
        Document document = new Document();
        document.add(new StoredField("_source", loadingContext.get().utf8ToString()));
        return document;
    }

    public BytesRef source() {
        return loadingContext.get();
    }

    @Override
    public String toString() {
        return "source";
    }
}