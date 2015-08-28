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
package org.elasticsearch.search.fetch.fielddata;

import org.elasticsearch.search.fetch.FetchSubPhaseContext;

import java.util.ArrayList;
import java.util.List;

/**
 * All the required context to pull a field from the field data cache.
 */
public class FieldDataFieldsContext extends FetchSubPhaseContext {

    public static class FieldDataField {
        private final String name;

        public FieldDataField(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }
    }

    private List<FieldDataField> fields = new ArrayList<>();

    public FieldDataFieldsContext() {
    }

    public void add(FieldDataField field) {
        this.fields.add(field);
    }

    public List<FieldDataField> fields() {
        return this.fields;
    }
}
