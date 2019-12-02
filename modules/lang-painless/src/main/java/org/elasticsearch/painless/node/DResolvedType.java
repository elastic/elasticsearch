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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.util.Objects;

public class DResolvedType extends DType {

    protected final Class<?> type;
    protected final boolean check;

    public DResolvedType(Location location, Class<?> type) {
        this(location, type, true);
    }

    public DResolvedType(Location location, Class<?> type, boolean check) {
        super(location);
        this.type = Objects.requireNonNull(type);
        this.check = check;
    }

    @Override
    public DResolvedType resolveType(PainlessLookup painlessLookup) {
        if (check == true) {
            if (painlessLookup.getClasses().contains(type) == false) {
                throw location.createError(new IllegalArgumentException(
                        "cannot resolve type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "]"));
            }

            return new DResolvedType(location, type, false);
        } else {
            return this;
        }
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    @Override
    public String toString() {
        return " (DResolvedType [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "])";
    }
}
