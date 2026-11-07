/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * The cluster-wide refusal for a definition no node could read off the wire. It is decided while the query is
 * analyzed, so a cluster that cannot carry one refuses the whole query rather than half of it.
 */
public class InlineDatasetDefinitionTests extends ESTestCase {

    private static final TransportVersion SUPPORTED_FROM = TransportVersion.fromName("esql_inline_dataset_definition");

    public void testAPlanCarryingADefinitionIsRefusedOnAnOlderCluster() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(SUPPORTED_FROM);

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> InlineDatasetDefinition.ensureEveryDefinitionCanTravel(planWithDefinition(), before)
        );
        assertThat(e.getMessage(), containsString("not supported on all nodes in the cluster"));
    }

    public void testAPlanCarryingADefinitionPassesOnceEveryNodeCanReadOne() {
        InlineDatasetDefinition.ensureEveryDefinitionCanTravel(planWithDefinition(), SUPPORTED_FROM);
        InlineDatasetDefinition.ensureEveryDefinitionCanTravel(planWithDefinition(), TransportVersion.current());
    }

    /** The refusal is about the definition, not about external reads: a plan without one is unaffected. */
    public void testAPlanWithNoDefinitionIsNotRefused() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(SUPPORTED_FROM);

        InlineDatasetDefinition.ensureEveryDefinitionCanTravel(registeredPlan(), before);
        InlineDatasetDefinition.ensureEveryDefinitionCanTravel(indexPlan(), before);
    }

    private static LogicalPlan planWithDefinition() {
        return relation(new InlineDatasetDefinition("s3_source", Map.of("resource", "s3://bucket/*.csv")));
    }

    private static LogicalPlan registeredPlan() {
        return relation(null);
    }

    private static LogicalPlan relation(InlineDatasetDefinition inline) {
        return new UnresolvedExternalRelation(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "s3://bucket/*.csv"),
            Map.of(),
            List.of(),
            inline == null ? "logs" : null,
            null,
            inline
        );
    }

    private static LogicalPlan indexPlan() {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, "logs"), false, List.of(), IndexMode.STANDARD, null);
    }
}
