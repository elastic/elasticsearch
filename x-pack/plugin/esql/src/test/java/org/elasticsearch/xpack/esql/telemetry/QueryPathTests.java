/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class QueryPathTests extends ESTestCase {

    public void testKibanaSourceIsTheFeature() {
        Feature feature = randomFrom(Feature.values());
        LeadingCommand leadingCommand = randomFrom(LeadingCommand.values());
        QueryPath path = QueryPath.of(new QueryCaller(QueryMetric.KIBANA, feature), null, leadingCommand);
        assertThat(path, equalTo(new QueryPath(QueryMetric.KIBANA, feature.toString(), leadingCommand)));
    }

    public void testRestSourceIsTheDirectClient() {
        DirectClient directClient = randomFrom(DirectClient.values());
        LeadingCommand leadingCommand = randomFrom(LeadingCommand.values());
        QueryPath path = QueryPath.of(new QueryCaller(QueryMetric.REST, null), directClient, leadingCommand);
        assertThat(path, equalTo(new QueryPath(QueryMetric.REST, directClient.toString(), leadingCommand)));
    }

    public void testOtherCallersHaveNoSource() {
        for (QueryMetric caller : QueryMetric.values()) {
            if (caller == QueryMetric.KIBANA || caller == QueryMetric.REST) {
                continue;
            }
            QueryPath path = QueryPath.of(new QueryCaller(caller, null), null, LeadingCommand.FROM);
            assertThat(path.source(), equalTo(QueryPath.NO_SOURCE));
        }
    }

    /**
     * Pins the size of the tree in {@code _xpack/usage}: each path reports at most {@code total}, {@code failed} and one count per
     * {@link FeatureMetric}. A new caller, Kibana feature, direct client or leading command grows it, and has to update this cap.
     */
    public void testNumberOfPossiblePathsIsCapped() {
        Set<QueryPath> paths = new HashSet<>();
        for (QueryMetric client : QueryMetric.values()) {
            for (Feature feature : Feature.values()) {
                for (DirectClient directClient : DirectClient.values()) {
                    for (LeadingCommand leadingCommand : LeadingCommand.values()) {
                        paths.add(
                            QueryPath.of(
                                new QueryCaller(client, client == QueryMetric.KIBANA ? feature : null),
                                directClient,
                                leadingCommand
                            )
                        );
                    }
                }
            }
        }
        assertThat(paths, hasSize(256));
        assertThat(paths.size() * (2 + FeatureMetric.values().length), equalTo(12_032));
    }

    public void testNoLevelValueClashesWithAKeyOrHoldsADot() {
        Set<String> keys = Set.of(QueryPath.TOTAL, QueryPath.FAILED, QueryPath.COMMANDS);
        List<String> values = new ArrayList<>();
        Arrays.stream(QueryMetric.values()).map(QueryMetric::toString).forEach(values::add);
        Arrays.stream(Feature.values()).map(Feature::toString).forEach(values::add);
        Arrays.stream(DirectClient.values()).map(DirectClient::toString).forEach(values::add);
        Arrays.stream(LeadingCommand.values()).map(LeadingCommand::toString).forEach(values::add);
        Arrays.stream(FeatureMetric.values()).map(FeatureMetric::toString).forEach(values::add);
        values.add(QueryPath.NO_SOURCE);
        for (String value : values) {
            assertThat(value, not(containsString(".")));
            assertFalse(value, keys.contains(value));
        }
    }
}
