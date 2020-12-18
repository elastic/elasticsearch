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
import org.elasticsearch.index.query.QueryShardContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * All the required context to pull a field from the doc values.
 * This contains:
 * <ul>
 *   <li>a list of field names and its format.
 * </ul>
 */
public class FetchDocValuesContext {

    private final List<FieldAndFormat> fields = new ArrayList<>();

    /**
     * Create a new FetchDocValuesContext using the provided input list.
     * Field patterns containing wildcards are resolved and unmapped fields are filtered out.
     */
    public FetchDocValuesContext(QueryShardContext shardContext, List<FieldAndFormat> fieldPatterns) {
        for (FieldAndFormat field : fieldPatterns) {
            Collection<String> fieldNames = shardContext.simpleMatchToIndexNames(field.field);
            for (String fieldName : fieldNames) {
                if (shardContext.isFieldMapped(fieldName)) {
                    fields.add(new FieldAndFormat(fieldName, field.format, field.includeUnmapped));
                }
            }
        }

        int maxAllowedDocvalueFields = shardContext.getIndexSettings().getMaxDocvalueFields();
        if (fields.size() > maxAllowedDocvalueFields) {
            throw new IllegalArgumentException(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxAllowedDocvalueFields
                    + "] but was [" + fields.size() + "]. This limit can be set by changing the ["
                    + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting.");
        }
    }

    /**
     * Returns the required docvalue fields.
     */
    public List<FieldAndFormat> fields() {
        return this.fields;
    }
}
