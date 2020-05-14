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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessLookup;

import java.util.Objects;

/**
 * Represents a canonical Painless type name as a {@link String}
 * that requires resolution.
 */
public class DUnresolvedType extends DType {

    protected final String canonicalTypeName;

    public DUnresolvedType(Location location, String typeName) {
        super(location);
        this.canonicalTypeName = Objects.requireNonNull(typeName);
    }

    /**
     * Resolves the canonical Painless type name to a Painless type.
     * @throws IllegalArgumentException if the type cannot be resolved from the {@link PainlessLookup}
     * @return a {@link DResolvedType} where the resolved Painless type is retrievable
     */
    @Override
    public DResolvedType resolveType(PainlessLookup painlessLookup) {
        Class<?> type = painlessLookup.canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw location.createError(new IllegalArgumentException("cannot resolve type [" + canonicalTypeName + "]"));
        }

        return new DResolvedType(location, type);
    }

    /** @return the canonical Painless type name */
    @Override
    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }
}
 
