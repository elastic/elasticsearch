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

package org.elasticsearch.plugin.example;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

/**
 * Scripts can be used in the search context (in script query or script field) and/or no-search context (update by script, ingest node).
 * Depending on the purpose of the script you can implement support for either or both contexts.
 */
public interface ExampleScriptFactory {

    /**
     * This method is called in non-search context such as update by script or in ingest node
     */
    default ExecutableScript executable(@Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException("the script cannot be used in non-search context");
    }

    /**
     * This method is called in search context such as script query or script field. Therefore the script has access
     * to the search results it is running on.
     */
    default SearchScript search(SearchLookup lookup, @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException("the script cannot be used in search context");
    }
}
