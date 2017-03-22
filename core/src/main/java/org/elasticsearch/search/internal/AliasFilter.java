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

package org.elasticsearch.search.internal;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a {@link QueryBuilder} and a list of alias names that filters the builder is composed of.
 */
public final class AliasFilter implements Writeable {

    private final String[] aliases;
    private final QueryBuilder filter;
    private final boolean reparseAliases;

    public AliasFilter(QueryBuilder filter, String... aliases) {
        this.aliases = aliases == null ? Strings.EMPTY_ARRAY : aliases;
        this.filter = filter;
        reparseAliases = false; // no bwc here - we only do this if we parse the filter
    }

    public AliasFilter(StreamInput input) throws IOException {
        aliases = input.readStringArray();
        if (input.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            filter = input.readOptionalNamedWriteable(QueryBuilder.class);
            reparseAliases = false;
        } else {
            reparseAliases = true; // alright we read from 5.0
            filter = null;
        }
    }

    private QueryBuilder reparseFilter(QueryRewriteContext context) {
        if (reparseAliases) {
            // we are processing a filter received from a 5.0 node - we need to reparse this on the executing node
            final IndexMetaData indexMetaData = context.getIndexSettings().getIndexMetaData();
            /* Being static, parseAliasFilter doesn't have access to whatever guts it needs to parse a query. Instead of passing in a bunch
             * of dependencies we pass in a function that can perform the parsing. */
            CheckedFunction<byte[], QueryBuilder, IOException> filterParser = bytes -> {
                try (XContentParser parser = XContentFactory.xContent(bytes).createParser(context.getXContentRegistry(), bytes)) {
                    return context.newParseContext(parser).parseInnerQueryBuilder();
                }
            };
            return ShardSearchRequest.parseAliasFilter(filterParser, indexMetaData, aliases);
        }
        return filter;
    }

    AliasFilter rewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder queryBuilder = reparseFilter(context);
        if (queryBuilder != null) {
            return new AliasFilter(QueryBuilder.rewriteQuery(queryBuilder, context), aliases);
        }
        return new AliasFilter(filter, aliases);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(aliases);
        if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            out.writeOptionalNamedWriteable(filter);
        }
    }

    /**
     * Returns the aliases patters that are used to compose the {@link QueryBuilder}
     * returned from {@link #getQueryBuilder()}
     */
    public String[] getAliases() {
        return aliases;
    }

    /**
     * Returns the alias filter {@link QueryBuilder} or <code>null</code> if there is no such filter
     */
    public QueryBuilder getQueryBuilder() {
        if (reparseAliases) {
            // this is only for BWC since 5.0 still  only sends aliases so this must be rewritten on the executing node
            // if we talk to an older node we also only forward/write the string array which is compatible with the consumers
            // in 5.0 see ExplainRequest and QueryValidationRequest
            throw new IllegalStateException("alias filter for aliases: " + Arrays.toString(aliases) + " must be rewritten first");
        }
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasFilter that = (AliasFilter) o;
        return reparseAliases == that.reparseAliases &&
            Arrays.equals(aliases, that.aliases) &&
            Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases, filter, reparseAliases);
    }

    @Override
    public String toString() {
        return "AliasFilter{" +
            "aliases=" + Arrays.toString(aliases) +
            ", filter=" + filter +
            ", reparseAliases=" + reparseAliases +
            '}';
    }
}
