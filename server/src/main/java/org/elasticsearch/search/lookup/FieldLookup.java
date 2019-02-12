/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.search.lookup;

import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldLookup {

    // we can cached fieldType completely per name, since its on an index/shard level (the lookup, and it does not change within the scope
    // of a search request)
    private final MappedFieldType fieldType;

    private Map<String, List<Object>> fields;

    private Object value;

    private boolean valueLoaded = false;

    private List<Object> values = new ArrayList<>();

    private boolean valuesLoaded = false;

    FieldLookup(MappedFieldType fieldType) {
        this.fieldType = fieldType;
    }

    public MappedFieldType fieldType() {
        return fieldType;
    }

    public Map<String, List<Object>> fields() {
        return fields;
    }

    /**
     * Sets the post processed values.
     */
    public void fields(Map<String, List<Object>> fields) {
        this.fields = fields;
    }

    public void clear() {
        value = null;
        valueLoaded = false;
        values.clear();
        valuesLoaded = false;
        fields = null;
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
        List<Object> values = fields.get(fieldType.name());
        return values != null ? value = values.get(0) : null;
    }

    public List<Object> getValues() {
        if (valuesLoaded) {
            return values;
        }
        valuesLoaded = true;
        values.clear();
        return values = fields().get(fieldType.name());
    }
}
