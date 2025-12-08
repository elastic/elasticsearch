/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;

public class EsqlResolvedIndexExpressionIT extends AbstractCrossClusterTestCase {

    public void testLocalIndices() {
        createIndex(LOCAL_CLUSTER, "index-1");

        assertThat(resolveIndices("index-1"), hasEntry(LOCAL_CLUSTER, resolvedIndexExpression("index-1", "index-1")));
    }

    public void testLocalAlias() {
        createIndex(LOCAL_CLUSTER, "index-1");
        createAlias(LOCAL_CLUSTER, "alias-1", "index-1");

        assertThat(resolveIndices("alias-1"), hasEntry(LOCAL_CLUSTER, resolvedIndexExpression("alias-1", "index-1")));
    }

    public void testLocalPattern() {
        createIndex(LOCAL_CLUSTER, "index-1");
        createIndex(LOCAL_CLUSTER, "index-2");

        assertThat(resolveIndices("index-*"), hasEntry(LOCAL_CLUSTER, resolvedIndexExpression("index-*", "index-1,index-2")));
    }

    public void testLocalMultiple() {
        createIndex(LOCAL_CLUSTER, "index-1");
        createIndex(LOCAL_CLUSTER, "index-2");

        assertThat(
            resolveIndices("index-1,index-2"),
            hasEntry(LOCAL_CLUSTER, resolvedIndexExpression("index-1,index-2", "index-1,index-2"))
        );
    }

    public void testLocalAndRemote() {
        createIndex(LOCAL_CLUSTER, "index-1");
        createIndex(REMOTE_CLUSTER_1, "index-2");
        createIndex(REMOTE_CLUSTER_2, "index-3");

        assertThat(
            resolveIndices("index-*,*:index-*"),
            allOf(
                hasEntry(LOCAL_CLUSTER, resolvedIndexExpression("index-*", "index-1")),
                hasEntry(REMOTE_CLUSTER_1, resolvedIndexExpression("index-*", "index-2")),
                hasEntry(REMOTE_CLUSTER_2, resolvedIndexExpression("index-*", "index-3"))
            )
        );
    }

    private Map<String, EsqlResolvedIndexExpression> resolveIndices(String indices) {
        return EsqlResolvedIndexExpression.from(
            client(LOCAL_CLUSTER).prepareFieldCaps(Strings.splitStringByCommaToArray(indices))
                .setFields("*")
                .setIncludeResolvedTo(true)
                .get()
        );
    }

    private void createIndex(String clusterAlias, String index) {
        client(clusterAlias).admin().indices().prepareCreate(index).get();
    }

    private void createAlias(String clusterAlias, String alias, String index) {
        client(clusterAlias).admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(index, alias).get();
    }

    private static EsqlResolvedIndexExpression resolvedIndexExpression(String expression, String resolved) {
        return new EsqlResolvedIndexExpression(
            Set.of(Strings.splitStringByCommaToArray(expression)),
            Set.of(Strings.splitStringByCommaToArray(resolved))
        );
    }
}
