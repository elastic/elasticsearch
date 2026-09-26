/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.junit.Before;

import java.util.List;
import java.util.Map;

/**
 * Exercises cross-cluster search against a remote with {@code skip_unavailable: false}, which the shared base class can only express
 * per-class rather than per-method.
 */
public class MissingRemoteIndexCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String TEXT_FIELD = "text-field";
    private static final String MISSING_INDEX_NAME = "missing-index";
    private static final String DOC_ID = "doc-1";
    private static final String FIELD_VALUE = "value";

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, false);
    }

    @Before
    public void setupClusters() throws Exception {
        final Map<String, Object> mappings = Map.of(TEXT_FIELD, textMapping());
        final Map<String, Map<String, Object>> docs = Map.of(DOC_ID, Map.of(TEXT_FIELD, FIELD_VALUE));
        setupTwoClusters(
            new TestIndexInfo(LOCAL_INDEX_NAME, Map.of(), mappings, docs),
            new TestIndexInfo(REMOTE_INDEX_NAME, Map.of(), mappings, docs)
        );
    }

    public void testMissingRemoteIndexFailsWhenRemoteIsNotSkippable() {
        // The remote inference lookup tolerates a missing index, so the search's own resolution is the only thing left to reject one.
        // With skip_unavailable: false that rejection must still surface.
        assertSearchFailure(
            new MatchQueryBuilder(TEXT_FIELD, FIELD_VALUE),
            List.of(LOCAL_INDEX_NAME, fullyQualifiedIndexName(REMOTE_CLUSTER, MISSING_INDEX_NAME)),
            IndexNotFoundException.class,
            "no such index [" + MISSING_INDEX_NAME + "]",
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }
}
