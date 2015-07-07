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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
 * matches any character sequence (including the empty one), and <tt>?</tt>,
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
 * <tt>?</tt>.
 */
public class WildcardQueryBuilder extends AbstractQueryBuilder<WildcardQueryBuilder> implements MultiTermQueryBuilder<WildcardQueryBuilder> {

    public static final String NAME = "wildcard";

    private final String name;

    private final String wildcard;

    private String rewrite;

    static final WildcardQueryBuilder PROTOTYPE = new WildcardQueryBuilder(null, null);

    /**
     * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
     * matches any character sequence (including the empty one), and <tt>?</tt>,
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
     * <tt>?</tt>.
     *
     * @param name     The field name
     * @param wildcard The wildcard query string
     */
    public WildcardQueryBuilder(String name, String wildcard) {
        this.name = name;
        this.wildcard = wildcard;
    }

    public WildcardQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(name);
        builder.field("wildcard", wildcard);
        if (rewrite != null) {
            builder.field("rewrite", rewrite);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
