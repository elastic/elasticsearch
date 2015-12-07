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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;

public interface ParameterParser<T extends ToXContent> {
    /**
     * Creates a new item from the json held by the {@link ParameterParser}
     * in {@link org.elasticsearch.common.xcontent.XContent} format
     *
     * @param context
     *            the input parse context. The state on the parser contained in
     *            this context will be changed as a side effect of this method
     *            call
     * @return the new item
     */
    T fromXContent(QueryParseContext context, String elementName) throws IOException;
}
