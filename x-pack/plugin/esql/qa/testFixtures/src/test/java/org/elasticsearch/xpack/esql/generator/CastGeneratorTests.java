/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.generator.function.CastGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class CastGeneratorTests extends ESTestCase {

    private static final int ITERATIONS = 500;

    /**
     * Neither {@code long} nor {@code ip} converts to the other, and both convert to {@code keyword}.
     */
    private static final Column LONG_AND_IP = new Column("f", "unsupported", List.of("long", "ip"));

    public void testStrictCastsOnlyWhenEveryTypeConverts() {
        Set<DataType> strictTargets = new HashSet<>();
        Set<DataType> lenientTargets = new HashSet<>();
        for (int i = 0; i < ITERATIONS; i++) {
            CastGenerator.Cast cast = CastGenerator.randomCast(List.of(LONG_AND_IP), true);
            assertNotNull(cast);
            (cast.lenient() ? lenientTargets : strictTargets).add(cast.target());
        }
        assertThat(strictTargets, hasItem(DataType.KEYWORD));
        assertThat(strictTargets, not(hasItem(DataType.LONG)));
        assertThat(strictTargets, not(hasItem(DataType.IP)));
        // long converts to long, ip does not, and the other way around, so only the lenient cast reaches them.
        assertThat(lenientTargets, hasItem(DataType.LONG));
        assertThat(lenientTargets, hasItem(DataType.IP));
        assertThat(lenientTargets, hasItem(DataType.KEYWORD));
        // Neither long nor ip converts to version.
        assertThat(lenientTargets, not(hasItem(DataType.VERSION)));
    }

    public void testEveryGeneratedCastIsExpectedToResolve() {
        List<Column> columns = List.of(
            LONG_AND_IP,
            new Column("k", "keyword", List.of()),
            new Column("b", "boolean", List.of()),
            new Column("d", "unsupported", List.of("datetime", "boolean")),
            new Column("weird-name", "integer", List.of())
        );
        for (int i = 0; i < ITERATIONS; i++) {
            CastGenerator.Cast cast = CastGenerator.randomCast(columns, randomBoolean());
            assertNotNull(cast);
            long converting = cast.sourceTypes().stream().filter(source -> CastGenerator.converts(cast.target(), source)).count();
            if (cast.lenient()) {
                assertTrue(cast.expression(), converting > 0);
                assertThat(cast.expression(), containsString("::~" + cast.target().typeName()));
            } else {
                assertThat(cast.expression(), converting, equalTo((long) cast.sourceTypes().size()));
                assertThat(cast.expression(), not(containsString("::~")));
            }
        }
    }

    public void testNoLenientCastsWhenNotAllowed() {
        for (int i = 0; i < ITERATIONS; i++) {
            CastGenerator.Cast cast = CastGenerator.randomCast(List.of(LONG_AND_IP), false);
            assertNotNull(cast);
            assertFalse(cast.expression(), cast.lenient());
            assertTrue(cast.expression(), CastGenerator.converts(cast.target(), DataType.LONG));
            assertTrue(cast.expression(), CastGenerator.converts(cast.target(), DataType.IP));
        }
    }

    public void testUnconvertibleBranchOnlyGetsLenientCasts() {
        // An object branch converts to nothing, so every cast must be lenient and convert the geo_shape branch.
        Column geoShapeAndObject = new Column("g", "unsupported", List.of("geo_shape", "object"));
        for (int i = 0; i < ITERATIONS; i++) {
            CastGenerator.Cast cast = CastGenerator.randomCast(List.of(geoShapeAndObject), true);
            assertNotNull(cast);
            assertTrue(cast.expression(), cast.lenient());
            assertTrue(cast.expression(), CastGenerator.converts(cast.target(), DataType.GEO_SHAPE));
        }
    }

    public void testNeverCastsToExcludedTargets() {
        // long converts to geotile, so geotile would be a candidate if it were not excluded.
        Column longColumn = new Column("l", "long", List.of());
        for (int i = 0; i < ITERATIONS; i++) {
            CastGenerator.Cast cast = CastGenerator.randomCast(List.of(longColumn), true);
            assertThat(cast.expression(), cast.target(), not(equalTo(DataType.GEOTILE)));
        }
    }

    public void testNoCastOfUncastableColumns() {
        List<Column> columns = List.of(
            new Column("u", "unsupported", List.of()),
            new Column("n", "null", List.of()),
            // An unsupported branch collapses the whole field before any cast applies.
            new Column("r", "unsupported", List.of("rank_feature", "keyword"))
        );
        for (int i = 0; i < ITERATIONS; i++) {
            assertNull(CastGenerator.randomCast(columns, true));
        }
    }

    public void testQuotesColumnNames() {
        for (int i = 0; i < ITERATIONS; i++) {
            CastGenerator.Cast cast = CastGenerator.randomCast(List.of(new Column("weird-name", "integer", List.of())), randomBoolean());
            assertThat(cast.expression(), containsString("`weird-name`"));
        }
    }
}
