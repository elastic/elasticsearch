/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexViewOriginsTests extends ESTestCase {

    public void testDescribeSingleView() {
        var origins = new IndexViewOrigins(Map.of("missing", Set.of(List.of("v1"))));
        assertThat(origins.describe("missing"), equalTo(Optional.of("referenced by view [v1]")));
    }

    public void testDescribeNestedViewChain() {
        var origins = new IndexViewOrigins(Map.of("missing", Set.of(List.of("outer", "middle", "inner"))));
        assertThat(origins.describe("missing"), equalTo(Optional.of("referenced by view [outer -> middle -> inner]")));
    }

    public void testDescribeSortsViews() {
        var origins = new IndexViewOrigins(Map.of("missing", Set.of(List.of("v2"), List.of("outer", "v1"), List.of("v1"))));
        assertThat(origins.describe("missing"), equalTo(Optional.of("referenced by views [outer -> v1], [v1], [v2]")));
    }

    public void testDescribeNothingWithoutViews() {
        var origins = new IndexViewOrigins(Map.of("missing", Set.of(List.of("v1"))));
        assertThat(origins.describe("other"), equalTo(Optional.empty()));
        assertThat(origins.describe(""), equalTo(Optional.empty()));
        assertThat(IndexViewOrigins.EMPTY.describe("missing"), equalTo(Optional.empty()));
    }

    public void testDescribeIgnoresDuplicateIndices() {
        var origins = new IndexViewOrigins(Map.of("missing", Set.of(List.of("v1"))));
        assertThat(origins.describe("missing,missing"), equalTo(Optional.of("referenced by view [v1]")));
    }

    public void testDescribeIgnoresExclusions() {
        var origins = new IndexViewOrigins(Map.of("missing", Set.of(List.of("v1")), "-other", Set.of(List.of("v2"))));
        assertThat(origins.describe("missing,-other,remote:-other"), equalTo(Optional.of("referenced by view [v1]")));
    }

    public void testDescribeIndicesSharingViewsOnce() {
        var origins = new IndexViewOrigins(Map.of("missing1", Set.of(List.of("v1")), "missing2", Set.of(List.of("v1"))));
        assertThat(origins.describe("missing1,missing2"), equalTo(Optional.of("referenced by view [v1]")));
    }

    public void testDescribeEachIndexWithDifferentViews() {
        var origins = new IndexViewOrigins(Map.of("missing1", Set.of(List.of("v1"), List.of("v2")), "missing2", Set.of(List.of("v2"))));
        assertThat(
            origins.describe("missing1,missing2"),
            equalTo(Optional.of("([missing1] referenced by views [v1], [v2]; [missing2] referenced by view [v2])"))
        );
    }

    public void testDescribeOnlyIndicesReachedThroughViews() {
        var origins = new IndexViewOrigins(Map.of("missing1", Set.of(List.of("v1"))));
        assertThat(origins.describe("missing1,missing2"), equalTo(Optional.of("([missing1] referenced by view [v1])")));
    }

    public void testBuilderSkipsIndicesNamedByQuery() {
        var builder = new IndexViewOrigins.Builder();
        builder.record("missing", List.of());
        assertThat(builder.build(), equalTo(IndexViewOrigins.EMPTY));
    }

    public void testBuilderRecordsEachIndex() {
        var builder = new IndexViewOrigins.Builder();
        builder.record("missing1,missing2", List.of("v1"));
        builder.record("missing1", List.of("v2"));
        assertThat(
            builder.build(),
            equalTo(new IndexViewOrigins(Map.of("missing1", Set.of(List.of("v1"), List.of("v2")), "missing2", Set.of(List.of("v1")))))
        );
    }
}
