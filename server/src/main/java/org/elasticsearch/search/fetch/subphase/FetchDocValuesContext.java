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
package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * All the required context to pull a field from the doc values.
 */
public class FetchDocValuesContext {

    private final List<FieldAndFormat> fields;

    public static FetchDocValuesContext create(MapperService mapperService,
                                               List<FieldAndFormat> fieldPatterns) {
        List<FieldAndFormat> fields = new ArrayList<>();
        for (FieldAndFormat field : fieldPatterns) {
            Collection<String> fieldNames = mapperService.simpleMatchToFullName(field.field);
            for (String fieldName: fieldNames) {
                fields.add(new FieldAndFormat(fieldName, field.format));
            }
        }
        int maxAllowedDocvalueFields = mapperService.getIndexSettings().getMaxDocvalueFields();
        if (fields.size() > maxAllowedDocvalueFields) {
            throw new IllegalArgumentException(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxAllowedDocvalueFields
                    + "] but was [" + fields.size() + "]. This limit can be set by changing the ["
                    + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting.");
        }

        return new FetchDocValuesContext(fields);
    }

    FetchDocValuesContext(List<FieldAndFormat> fields) {
        this.fields = fields;
    }

    /**
     * Returns the required docvalue fields
     */
    public List<FieldAndFormat> fields() {
        return this.fields;
    }
}
