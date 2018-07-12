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

package org.elasticsearch.painless.lookup;

import java.util.Objects;

/**
 * Key for looking up a method.
 * <p>
 * Methods are keyed on both name and arity, and can be overloaded once per arity.
 * This allows signatures such as {@code String.indexOf(String) vs String.indexOf(String, int)}.
 * <p>
 * It is less flexible than full signature overloading where types can differ too, but
 * better than just the name, and overloading types adds complexity to users, too.
 */
public final class PainlessMethodKey {
    public final String name;
    public final int arity;

    /**
     * Create a new lookup key
     * @param name name of the method
     * @param arity number of parameters
     */
    public PainlessMethodKey(String name, int arity) {
        this.name = Objects.requireNonNull(name);
        this.arity = arity;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + arity;
        result = prime * result + name.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        PainlessMethodKey other = (PainlessMethodKey) obj;
        if (arity != other.arity) return false;
        if (!name.equals(other.name)) return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append('/');
        sb.append(arity);
        return sb.toString();
    }
}
