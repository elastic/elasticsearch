/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ProjectRoutingUsageSnapshotTests extends AbstractWireSerializingTestCase<ProjectRoutingUsageSnapshot> {

    @Override
    protected Writeable.Reader<ProjectRoutingUsageSnapshot> instanceReader() {
        return ProjectRoutingUsageSnapshot::new;
    }

    @Override
    protected ProjectRoutingUsageSnapshot createTestInstance() {
        if (randomBoolean()) {
            return new ProjectRoutingUsageSnapshot();
        }
        return randomSnapshot();
    }

    static ProjectRoutingUsageSnapshot randomSnapshot() {
        return new ProjectRoutingUsageSnapshot(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected ProjectRoutingUsageSnapshot mutateInstance(ProjectRoutingUsageSnapshot instance) {
        // Pick one field to increment so the result is guaranteed to differ
        int field = randomIntBetween(0, 14);
        return new ProjectRoutingUsageSnapshot(
            field == 0 ? instance.getSearchQueriesTotal() + 1 : instance.getSearchQueriesTotal(),
            field == 1 ? instance.getSearchWithProjectRouting() + 1 : instance.getSearchWithProjectRouting(),
            field == 2 ? instance.getSearchWithAliasOrigin() + 1 : instance.getSearchWithAliasOrigin(),
            field == 3 ? instance.getSearchWithAliasWildcard() + 1 : instance.getSearchWithAliasWildcard(),
            field == 4 ? instance.getSearchWithCustomTags() + 1 : instance.getSearchWithCustomTags(),
            field == 5 ? instance.getSearchWithNamedExpression() + 1 : instance.getSearchWithNamedExpression(),
            field == 6 ? instance.getSearchProjectRoutingFailures() + 1 : instance.getSearchProjectRoutingFailures(),
            field == 7 ? instance.getEsqlQueriesTotal() + 1 : instance.getEsqlQueriesTotal(),
            field == 8 ? instance.getEsqlWithProjectRouting() + 1 : instance.getEsqlWithProjectRouting(),
            field == 9 ? instance.getEsqlWithAliasOrigin() + 1 : instance.getEsqlWithAliasOrigin(),
            field == 10 ? instance.getEsqlWithAliasWildcard() + 1 : instance.getEsqlWithAliasWildcard(),
            field == 11 ? instance.getEsqlWithCustomTags() + 1 : instance.getEsqlWithCustomTags(),
            field == 12 ? instance.getEsqlWithNamedExpression() + 1 : instance.getEsqlWithNamedExpression(),
            field == 13 ? instance.getEsqlWithSet() + 1 : instance.getEsqlWithSet(),
            field == 14 ? instance.getEsqlProjectRoutingFailures() + 1 : instance.getEsqlProjectRoutingFailures()
        );
    }

    // -----------------------------------------------------------------------
    // add() accumulation
    // -----------------------------------------------------------------------

    public void testAdd_emptyPlusNonEmpty() {
        ProjectRoutingUsageSnapshot empty = new ProjectRoutingUsageSnapshot();
        ProjectRoutingUsageSnapshot full = randomSnapshot();
        empty.add(full);
        assertThat(empty, equalTo(full));
    }

    public void testAdd_doubling() {
        ProjectRoutingUsageSnapshot snap = randomSnapshot();
        ProjectRoutingUsageSnapshot acc = new ProjectRoutingUsageSnapshot();
        acc.add(snap);
        acc.add(snap);

        assertThat(acc.getSearchQueriesTotal(), equalTo(snap.getSearchQueriesTotal() * 2));
        assertThat(acc.getSearchWithProjectRouting(), equalTo(snap.getSearchWithProjectRouting() * 2));
        assertThat(acc.getSearchWithAliasOrigin(), equalTo(snap.getSearchWithAliasOrigin() * 2));
        assertThat(acc.getSearchWithAliasWildcard(), equalTo(snap.getSearchWithAliasWildcard() * 2));
        assertThat(acc.getSearchWithCustomTags(), equalTo(snap.getSearchWithCustomTags() * 2));
        assertThat(acc.getSearchWithNamedExpression(), equalTo(snap.getSearchWithNamedExpression() * 2));
        assertThat(acc.getSearchProjectRoutingFailures(), equalTo(snap.getSearchProjectRoutingFailures() * 2));
        assertThat(acc.getEsqlQueriesTotal(), equalTo(snap.getEsqlQueriesTotal() * 2));
        assertThat(acc.getEsqlWithProjectRouting(), equalTo(snap.getEsqlWithProjectRouting() * 2));
        assertThat(acc.getEsqlWithAliasOrigin(), equalTo(snap.getEsqlWithAliasOrigin() * 2));
        assertThat(acc.getEsqlWithAliasWildcard(), equalTo(snap.getEsqlWithAliasWildcard() * 2));
        assertThat(acc.getEsqlWithCustomTags(), equalTo(snap.getEsqlWithCustomTags() * 2));
        assertThat(acc.getEsqlWithNamedExpression(), equalTo(snap.getEsqlWithNamedExpression() * 2));
        assertThat(acc.getEsqlWithSet(), equalTo(snap.getEsqlWithSet() * 2));
        assertThat(acc.getEsqlProjectRoutingFailures(), equalTo(snap.getEsqlProjectRoutingFailures() * 2));
    }

    public void testAdd_null_isNoop() {
        ProjectRoutingUsageSnapshot snap = randomSnapshot();
        ProjectRoutingUsageSnapshot copy = new ProjectRoutingUsageSnapshot();
        copy.add(snap);
        copy.add(null);
        assertThat(copy, equalTo(snap));
    }

    public void testAdd_twoSnapshots() {
        ProjectRoutingUsageSnapshot a = randomSnapshot();
        ProjectRoutingUsageSnapshot b = randomSnapshot();
        ProjectRoutingUsageSnapshot acc = new ProjectRoutingUsageSnapshot();
        acc.add(a);
        acc.add(b);

        assertThat(acc.getSearchQueriesTotal(), equalTo(a.getSearchQueriesTotal() + b.getSearchQueriesTotal()));
        assertThat(
            acc.getSearchProjectRoutingFailures(),
            equalTo(a.getSearchProjectRoutingFailures() + b.getSearchProjectRoutingFailures())
        );
        assertThat(acc.getEsqlWithSet(), equalTo(a.getEsqlWithSet() + b.getEsqlWithSet()));
        assertThat(acc.getEsqlProjectRoutingFailures(), equalTo(a.getEsqlProjectRoutingFailures() + b.getEsqlProjectRoutingFailures()));
    }

    // -----------------------------------------------------------------------
    // toXContent suppression rules
    // -----------------------------------------------------------------------

    public void testToXContent_allZero_emitsNothing() throws IOException {
        ProjectRoutingUsageSnapshot snap = new ProjectRoutingUsageSnapshot();
        String json = toJson(snap);
        assertThat(json, not(containsString("search")));
        assertThat(json, not(containsString("esql")));
    }

    public void testToXContent_searchOnly_emitsSearchNotEsql() throws IOException {
        ProjectRoutingUsageSnapshot snap = new ProjectRoutingUsageSnapshot(
            5L,
            3L,
            1L,
            0L,
            0L,
            0L,
            2L,  // search: total=5, with_pr=3, alias_origin=1, failures=2
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L  // esql: all zero
        );
        String json = toJson(snap);
        assertThat(json, containsString("\"search\""));
        assertThat(json, containsString("\"queries\":5"));
        assertThat(json, containsString("\"queries_project_routing\":3"));
        assertThat(json, containsString("\"alias_origin\":1"));
        assertThat(json, containsString("\"failures\":2"));
        assertThat(json, not(containsString("\"esql\"")));
    }

    public void testToXContent_esqlOnly_emitsEsqlNotSearch() throws IOException {
        ProjectRoutingUsageSnapshot snap = new ProjectRoutingUsageSnapshot(
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,   // search: all zero
            8L,
            4L,
            0L,
            2L,
            0L,
            0L,
            3L,
            1L  // esql: total=8, with_pr=4, alias_wildcard=2, in_SET=3, failures=1
        );
        String json = toJson(snap);
        assertThat(json, not(containsString("\"search\"")));
        assertThat(json, containsString("\"esql\""));
        assertThat(json, containsString("\"queries\":8"));
        assertThat(json, containsString("\"queries_project_routing\":4"));
        assertThat(json, containsString("\"alias_wildcard\":2"));
        assertThat(json, containsString("\"in_SET\":3"));
        assertThat(json, containsString("\"failures\":1"));
    }

    public void testToXContent_bothPresent() throws IOException {
        ProjectRoutingUsageSnapshot snap = new ProjectRoutingUsageSnapshot(10L, 5L, 0L, 0L, 0L, 0L, 0L, 7L, 3L, 0L, 0L, 0L, 0L, 1L, 0L);
        String json = toJson(snap);
        assertThat(json, containsString("\"search\""));
        assertThat(json, containsString("\"esql\""));
    }

    private static String toJson(ProjectRoutingUsageSnapshot snap) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        snap.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return Strings.toString(builder);
    }
}
