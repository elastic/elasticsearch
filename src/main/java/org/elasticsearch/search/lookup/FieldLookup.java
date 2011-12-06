/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FieldLookup {

    // we can cached mapper completely per name, since its on an index/shard level (the lookup, and it does not change within the scope of a search request)
    private final FieldMapper mapper;

    private Document doc;

    private Object value;

    private boolean valueLoaded = false;

    private List<Object> values = new ArrayList<Object>();

    private boolean valuesLoaded = false;

    FieldLookup(FieldMapper mapper) {
        this.mapper = mapper;
    }

    public FieldMapper mapper() {
        return mapper;
    }

    public Document doc() {
        return doc;
    }

    public void doc(Document doc) {
        this.doc = doc;
    }

    public void clear() {
        value = null;
        valueLoaded = false;
        values.clear();
        valuesLoaded = false;
        doc = null;
    }

    public boolean isEmpty() {
        if (valueLoaded) {
            return value == null;
        }
        if (valuesLoaded) {
            return values.isEmpty();
        }
        return getValue() == null;
    }

    public Object getValue() {
        if (valueLoaded) {
            return value;
        }
        valueLoaded = true;
        value = null;
        Fieldable field = doc.getFieldable(mapper.names().indexName());
        if (field == null) {
            return null;
        }
        value = mapper.value(field);
        return value;
    }

    public List<Object> getValues() {
        if (valuesLoaded) {
            return values;
        }
        valuesLoaded = true;
        values.clear();
        Fieldable[] fields = doc.getFieldables(mapper.names().indexName());
        for (Fieldable field : fields) {
            values.add(mapper.value(field));
        }
        return values;
    }
}
