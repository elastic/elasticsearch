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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;

import java.io.IOException;

/**
 *
 */
// LUCENE MONITOR: Monitor against FieldComparator#String
public class StringValFieldDataComparator extends FieldComparator {

    private final String fieldName;

    protected final FieldDataCache fieldDataCache;

    protected FieldData currentFieldData;

    private String[] values;

    private String bottom;

    public StringValFieldDataComparator(int numHits, String fieldName, FieldDataCache fieldDataCache) {
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;
        values = new String[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        final String val1 = values[slot1];
        final String val2 = values[slot2];
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }

        return val1.compareTo(val2);
    }

    @Override
    public int compareBottom(int doc) {
        final String val2 = currentFieldData.stringValue(doc);
        if (bottom == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return bottom.compareTo(val2);
    }

    @Override
    public void copy(int slot, int doc) {
        values[slot] = currentFieldData.stringValue(doc);
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
        currentFieldData = fieldDataCache.cache(FieldDataType.DefaultTypes.STRING, reader, fieldName);
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public Comparable value(int slot) {
        return values[slot];
    }
}
