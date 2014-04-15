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

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Builds custom analyzer configuration required to reproduce a built in
 * analyzer. Plugins that define builtin analyzers should implement this so
 * users can get the configuration require to rebuild the builtin analyzer as a
 * custom analyzer via the _analyze endpoint.
 */
public interface BuiltinAsCustom {
    /**
     * Builds custom analyzer configuration required to reproduce a built in
     * analyzer.
     * 
     * @param builder
     *            sync for the configuration
     * @param name
     *            name of the analyzer
     * @return did we recognize the name and build an analyzer?
     * @throws IOException
     *             if the builder throws it
     */
    public abstract boolean build(XContentBuilder builder, String name) throws IOException;
}