/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class RequestFilterGraftTests extends ESTestCase {

    private static final long NOW = 1_600_000_000_000L;
    private static final TransportVersion CURRENT = RequestFilterGraft.ESQL_REQUEST_FILTER_ON_DATASET;
    private static final TransportVersion TOO_OLD = TransportVersion.minimumCompatible();

    private static ExternalRelation relation() {
        List<Attribute> output = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", "file:///data.csv");
        return new ExternalRelation(Source.EMPTY, "file:///data.csv", metadata, output, FileList.UNRESOLVED, Map.of(), "ds");
    }

    public void testNullFilterLeavesPlanUnchanged() {
        ExternalRelation relation = relation();
        assertSame(relation, RequestFilterGraft.graft(relation, null, NOW, CURRENT));
    }

    public void testSupportedFilterIsGraftedAboveTheRelation() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterGraft.graft(relation, QueryBuilders.termQuery("a", 1), NOW, CURRENT);
        assertThat(result, instanceOf(Filter.class));
        assertThat(((Filter) result).child(), sameInstance(relation));
    }

    public void testWhollyUnsupportedFilterLeavesTheRelationUnfiltered() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterGraft.graft(relation, QueryBuilders.wildcardQuery("a", "x*"), NOW, CURRENT);
        assertSame(relation, result); // the filter folds to a no-op, so the relation is untouched
        assertWarnings(
            "The request filter on external dataset [ds] could not apply [wildcard]; it was skipped, so more rows may be returned"
        );
    }

    /**
     * Partial application: a filter mixing a supported term with an unsupported wildcard still grafts the term (the
     * source is NOT left wholly unfiltered), and warns only about the dropped wildcard.
     */
    public void testPartialFilterGraftsTheSupportedClauseAndWarnsOnTheRest() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterGraft.graft(
            relation,
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("a", 1)).must(QueryBuilders.wildcardQuery("a", "x*")),
            NOW,
            CURRENT
        );
        assertThat(result, instanceOf(Filter.class));
        assertThat(((Filter) result).child(), sameInstance(relation));
        assertWarnings(
            "The request filter on external dataset [ds] could not apply [wildcard]; it was skipped, so more rows may be returned"
        );
    }

    /** The critical version gate: below the feature version the graft is skipped, so no plan an old node can't read ships. */
    public void testOldMinimumVersionSkipsTheGraftEntirely() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterGraft.graft(relation, QueryBuilders.termQuery("a", 1), NOW, TOO_OLD);
        assertSame(relation, result);
        assertWarnings(
            "The request filter was not applied to external dataset(s) [ds] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered"
        );
    }
}
