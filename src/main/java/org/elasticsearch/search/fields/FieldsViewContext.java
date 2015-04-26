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

package org.elasticsearch.search.fields;

import java.util.Set;

public final class FieldsViewContext {

    private static ThreadLocal<FieldsViewContext> current = new ThreadLocal<>();

    public static void createAndSet(Set<String> indexedFieldNames, Set<String> fullFieldNames) {
        current.set(new FieldsViewContext(indexedFieldNames, fullFieldNames));
    }

    public static void clear() {
        current.remove();
    }

    public static FieldsViewContext current() {
        return current.get();
    }

    private final Set<String> fullFieldNames;
    private final Set<String> indexedFieldNames;

    private FieldsViewContext(Set<String> indexedFieldNames, Set<String> fullFieldNames) {
        this.indexedFieldNames = indexedFieldNames;
        this.fullFieldNames = fullFieldNames;
    }

    public Set<String> getFullFieldNames() {
        return fullFieldNames;
    }

    public Set<String> getIndexedFieldNames() {
        return indexedFieldNames;
    }
}
