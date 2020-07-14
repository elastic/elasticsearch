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

package org.elasticsearch.compat;

import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestRequest;

import java.util.Set;
import java.util.function.Function;

public class TypeConsumer implements Function<String, Boolean> {

    private final RestRequest request;
    private final Set<String> fieldNames;
    private boolean foundTypeInBody = false;

    public TypeConsumer(RestRequest request, String... fieldNames) {
        this.request = request;
        this.fieldNames = Set.of(fieldNames);
    }

    @Override
    public Boolean apply(String fieldName) {
        if (fieldNames.contains(fieldName)) {
            foundTypeInBody = true;
            return true;
        }
        return false;
    }

    public boolean hasTypes() {
        // TODO can params be types too? or _types?
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        return types.length > 0 || foundTypeInBody;
    }
}
