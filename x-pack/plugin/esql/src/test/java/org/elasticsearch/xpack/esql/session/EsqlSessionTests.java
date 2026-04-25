/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.index.MappingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;

public class EsqlSessionTests extends ESTestCase {

    public void testShouldRetryConcreteTimeSeriesResolution() {
        assertTrue(
            EsqlSession.shouldRetryConcreteTimeSeriesResolution(
                IndexMode.TIME_SERIES,
                IndexResolution.empty("logs"),
                new IndexPattern(EMPTY, "logs")
            )
        );
    }

    public void testShouldNotRetryWildcardTimeSeriesResolution() {
        assertFalse(
            EsqlSession.shouldRetryConcreteTimeSeriesResolution(
                IndexMode.TIME_SERIES,
                IndexResolution.empty("logs*"),
                new IndexPattern(EMPTY, "logs*")
            )
        );
    }

    public void testRefineConcreteTimeSeriesResolutionReturnsHelpfulError() {
        IndexResolution resolution = EsqlSession.refineConcreteTimeSeriesResolution(
            new IndexPattern(EMPTY, "logs"),
            IndexResolution.empty("logs"),
            resolvedIndex("logs")
        );

        MappingException e = expectThrows(MappingException.class, resolution::get);
        assertThat(e.getMessage(), containsString("[logs] is not a time series index. Use FROM command instead"));
    }

    public void testRefineConcreteTimeSeriesResolutionKeepsOriginalFailures() {
        FieldCapabilitiesFailure failure = new FieldCapabilitiesFailure(new String[] { "logs" }, new ElasticsearchException("boom"));
        IndexResolution originalResolution = IndexResolution.valid(
            new EsIndex("logs", Map.of(), Map.of(), Map.of(), Map.of(), Map.of()),
            Set.of(),
            Map.of("remote", List.of(failure))
        );

        IndexResolution resolution = EsqlSession.refineConcreteTimeSeriesResolution(
            new IndexPattern(EMPTY, "logs"),
            originalResolution,
            IndexResolution.empty("logs")
        );

        assertThat(resolution, sameInstance(originalResolution));
    }

    private static IndexResolution resolvedIndex(String indexName) {
        return IndexResolution.valid(
            new EsIndex(indexName, Map.of(), Map.of(indexName, IndexMode.STANDARD), Map.of(), Map.of(), Map.of()),
            Set.of(indexName),
            Map.of()
        );
    }
}
