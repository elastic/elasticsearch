/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Used for detecting loops caused by fields referring to other fields. The chain is empty
 * for the "top level" lookup created for the entire search. When a lookup is used to load
 * fielddata for a field, we fork it and make sure the field name name isn't in the chain,
 * then add it to the end. So the lookup for the a field named {@code a} will be {@code ["a"]}. If
 * that field looks up the values of a field named {@code b} then
 * {@code b}'s chain will contain {@code ["a", "b"]}.
 */
public final class TrackingMappedFieldsLookup {

    /**
     * The maximum depth of field dependencies.
     * When a runtime field's doc values depends on another runtime field's doc values,
     * which depends on another runtime field's doc values and so on, it can
     * make a very deep stack, which we want to limit.
     */
    public static final int MAX_FIELD_CHAIN_DEPTH = 5;

    final Function<String, MappedFieldType> lookup;
    final Set<String> references;

    public TrackingMappedFieldsLookup(Function<String, MappedFieldType> lookup) {
        this.lookup = lookup;
        this.references = Collections.emptySet();
    }

    private TrackingMappedFieldsLookup(Function<String, MappedFieldType> lookup, Set<String> references, String field) {
        this.lookup = lookup;
        this.references = new LinkedHashSet<>(references);
        if (this.references.add(field) == false) {
            String message = String.join(" -> ", this.references) + " -> " + field;
            throw new IllegalArgumentException("Cyclic dependency detected while resolving runtime fields: " + message);
        }
        if (this.references.size() > MAX_FIELD_CHAIN_DEPTH) {
            throw new IllegalArgumentException("Field requires resolving too many dependent fields: "
                + String.join(" -> ", this.references));
        }
    }

    public TrackingMappedFieldsLookup trackingField(String field) {
        return new TrackingMappedFieldsLookup(lookup, this.references, field);
    }

    public MappedFieldType get(String field) {
        return lookup.apply(field);
    }

}
