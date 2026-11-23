/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * {@code split_queries_on_whitespace} on a keyword field must split the query whether the field uses the built-in
 * {@code lowercase} normalizer or an equivalent custom one.
 */
public class KeywordLowercaseNormalizerSplitIT extends ESSingleNodeTestCase {

    public void testBuiltinAndCustomLowercaseNormalizersSplitAlike() {
        indicesAdmin().prepareCreate("test")
            .setSettings(
                Settings.builder()
                    .put("index.analysis.normalizer.custom_lc.type", "custom")
                    .putList("index.analysis.normalizer.custom_lc.filter", "lowercase")
            )
            .setMapping("""
                {"properties": {
                  "builtin": {"type": "keyword", "normalizer": "lowercase", "split_queries_on_whitespace": true},
                  "custom": {"type": "keyword", "normalizer": "custom_lc", "split_queries_on_whitespace": true}
                }}""")
            .get();
        prepareIndex("test").setSource("builtin", "Foo", "custom", "Foo").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        for (String field : new String[] { "builtin", "custom" }) {
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(field, "FOO bar")), 1);
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(field, "FOO bar").operator(Operator.AND)), 0);
        }
    }
}
