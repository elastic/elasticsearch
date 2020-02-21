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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;

import java.util.List;

public interface ExpressionResolver {

    /**
     * Resolves the list of expressions into other expressions if possible (possible concrete indices and aliases, but
     * that isn't required). The provided implementations can also be left untouched.
     *
     * @return a new list with expressions based on the provided expressions
     */
    List<String> resolve(Context context, List<String> expressions);

}
