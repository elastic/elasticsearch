/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.LocationTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class UnresolvedRelationTests extends ESTestCase {
    public void testEqualsAndHashCode() {
        Location location = new Location(between(1, 1000), between(1, 1000));
        TableIdentifier table = new TableIdentifier(location, randomAlphaOfLength(5), randomAlphaOfLength(5));
        String alias = randomBoolean() ? null : randomAlphaOfLength(5);
        String unresolvedMessage = randomAlphaOfLength(5);
        UnresolvedRelation relation = new UnresolvedRelation(location, table, alias, unresolvedMessage);
        List<Function<UnresolvedRelation, UnresolvedRelation>> mutators = new ArrayList<>();
        mutators.add(r -> new UnresolvedRelation(
            LocationTests.mutate(r.location()),
            r.table(),
            r.alias(),
            r.unresolvedMessage()));
        mutators.add(r -> new UnresolvedRelation(
            r.location(),
            new TableIdentifier(r.location(), r.table().cluster(), r.table().index() + "m"),
            r.alias(),
            r.unresolvedMessage()));
        mutators.add(r -> new UnresolvedRelation(
            r.location(),
            r.table(),
            randomValueOtherThanMany(
                a -> Objects.equals(a, r.alias()),
                () -> randomBoolean() ? null : randomAlphaOfLength(5)),
            r.unresolvedMessage()));
        mutators.add(r -> new UnresolvedRelation(
            r.location(),
            r.table(),
            r.alias(),
            randomValueOtherThan(r.unresolvedMessage(), () -> randomAlphaOfLength(5))));
        checkEqualsAndHashCode(relation,
            r -> new UnresolvedRelation(r.location(), r.table(), r.alias(), r.unresolvedMessage()),
            r -> randomFrom(mutators).apply(r));
    }
}
