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

package org.elasticsearch.index.field.data.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;

import java.io.IOException;

/**
 *
 */
public abstract class NumericFieldDataComparator extends FieldComparator {

    private final String fieldName;

    protected final FieldDataCache fieldDataCache;

    protected NumericFieldData currentFieldData;

    public NumericFieldDataComparator(String fieldName, FieldDataCache fieldDataCache) {
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;
    }

    public abstract FieldDataType fieldDataType();

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
        currentFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType(), reader, fieldName);
    }
}