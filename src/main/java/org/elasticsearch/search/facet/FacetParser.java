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
package org.elasticsearch.search.facet;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A facet parser parses the relevant matching "type" of facet into a {@link FacetExecutor}.
 * <p/>
 * The parser also suggest the default {@link FacetExecutor.Mode} both for global and main executions.
 */
public interface FacetParser {

    /**
     * The type of the facet, for example, terms.
     */
    String[] types();

    /**
     * The default mode to use when executed as a "main" (query level) facet.
     */
    FacetExecutor.Mode defaultMainMode();

    /**
     * The default mode to use when executed as a "global" (all docs) facet.
     */
    FacetExecutor.Mode defaultGlobalMode();

    /**
     * Parses the facet into a {@link FacetExecutor}.
     */
    FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException;
}
