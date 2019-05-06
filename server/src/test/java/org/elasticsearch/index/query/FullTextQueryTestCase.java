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

package org.elasticsearch.index.query;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class FullTextQueryTestCase<QB extends AbstractQueryBuilder<QB>> extends AbstractQueryTestCase<QB> {
    protected abstract boolean isCacheable(QB queryBuilder);

    /**
     * Full text queries that start with "now" are not cacheable if they
     * target a {@link DateFieldMapper.DateFieldType} field.
     */
    protected final boolean isCacheable(Collection<String> fields, String value) {
        if (value.length() < 3
                || value.substring(0, 3).equalsIgnoreCase("now") == false) {
            return true;
        }
        Set<String> dateFields = new HashSet<>();
        getMapping().forEach(ft -> {
            if (ft instanceof DateFieldMapper.DateFieldType) {
                dateFields.add(ft.name());
            }
        });
        for (MappedFieldType ft : getMapping()) {
            if (ft instanceof DateFieldMapper.DateFieldType) {
                dateFields.add(ft.name());
            }
        }
        if (fields.isEmpty()) {
            // special case: all fields are requested
            return dateFields.isEmpty();
        }
        return fields.stream()
            .anyMatch(dateFields::contains) == false;
    }
}
