/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.ExternalPlanningReservation;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class SchemaInternerTests extends ESTestCase {

    public void testSameColumnKeyReusesTheFirstAttribute() {
        SchemaInterner interner = new SchemaInterner(null, 0);
        Attribute first = attribute("id", DataType.LONG, Nullability.TRUE, false);
        Attribute second = attribute("id", DataType.LONG, Nullability.TRUE, false);
        assertNotSame(first, second);

        List<Attribute> canonical = interner.canonicalize(List.of(first));
        assertSame(first, canonical.get(0));
        assertSame(first, interner.canonicalize(List.of(second)).get(0));
    }

    public void testDifferentTypeNullabilityOrSyntheticIsADifferentColumn() {
        SchemaInterner interner = new SchemaInterner(null, 0);
        Attribute amountLong = attribute("amount", DataType.LONG, Nullability.FALSE, false);
        Attribute amountDouble = attribute("amount", DataType.DOUBLE, Nullability.FALSE, false);
        Attribute nullable = attribute("amount", DataType.LONG, Nullability.TRUE, false);
        Attribute synthetic = attribute("amount", DataType.LONG, Nullability.FALSE, true);

        assertNotSame(interner.canonicalize(List.of(amountLong)).get(0), interner.canonicalize(List.of(amountDouble)).get(0));
        assertNotSame(interner.canonicalize(List.of(amountLong)).get(0), interner.canonicalize(List.of(nullable)).get(0));
        assertNotSame(interner.canonicalize(List.of(amountLong)).get(0), interner.canonicalize(List.of(synthetic)).get(0));
    }

    public void testSameSequenceReusesTheListAndALongerSequenceSharesPrefixAttributes() {
        SchemaInterner interner = new SchemaInterner(null, 0);
        Attribute id = attribute("id", DataType.LONG, Nullability.FALSE, false);
        Attribute name = attribute("name", DataType.KEYWORD, Nullability.FALSE, false);
        List<Attribute> first = interner.canonicalize(List.of(id, name));
        List<Attribute> second = interner.canonicalize(
            List.of(attribute("id", DataType.LONG, Nullability.FALSE, false), attribute("name", DataType.KEYWORD, Nullability.FALSE, false))
        );
        assertSame(first, second);

        List<Attribute> longer = interner.canonicalize(
            List.of(
                attribute("id", DataType.LONG, Nullability.FALSE, false),
                attribute("name", DataType.KEYWORD, Nullability.FALSE, false),
                attribute("extra", DataType.INTEGER, Nullability.FALSE, false)
            )
        );
        assertNotSame(first, longer);
        assertSame(first.get(0), longer.get(0));
        assertSame(first.get(1), longer.get(1));
    }

    public void testOverflowChargesOnlyTheExcessOverTheAllowance() {
        CircuitBreaker breaker = requestBreaker("1mb");
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker);
        // One column is 128 and its one-column shape is 40. 200 covers the first of each and only part of the next.
        long allowance = 200L;
        SchemaInterner interner = new SchemaInterner(reservation, allowance);
        interner.canonicalize(List.of(attribute("a", DataType.KEYWORD, Nullability.FALSE, false)));
        assertThat(reservation.queryHeld(), equalTo(0L));

        interner.canonicalize(List.of(attribute("b", DataType.LONG, Nullability.FALSE, false)));
        // retained after both shapes: 128+40+128+40 = 336. Overflow above 200 is 136. That is less than the
        // second canonicalize's full cost (128+40) and is not another 760 credit.
        long overflow = 336L - allowance;
        assertThat(reservation.queryHeld(), equalTo(overflow));
        assertThat(reservation.queryHeld(), lessThan(128L + 40L));
        assertThat(reservation.queryHeld(), lessThan(760L));
    }

    public void testBreakerRejectsTheNextColumnWithoutPublishingIt() {
        CircuitBreaker breaker = requestBreaker("40b");
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker);
        SchemaInterner interner = new SchemaInterner(reservation, 200L);
        List<Attribute> kept = interner.canonicalize(List.of(attribute("a", DataType.KEYWORD, Nullability.FALSE, false)));
        assertThat(reservation.queryHeld(), equalTo(0L));

        Attribute rejected = attribute("b", DataType.LONG, Nullability.FALSE, false);
        expectThrows(CircuitBreakingException.class, () -> interner.canonicalize(List.of(rejected)));
        assertThat(reservation.queryHeld(), equalTo(0L));

        List<Attribute> followUp = interner.canonicalize(List.of(attribute("a", DataType.KEYWORD, Nullability.FALSE, false)));
        assertSame(kept, followUp);
        assertNotSame(rejected, followUp.get(0));
    }

    private static Attribute attribute(String name, DataType type, Nullability nullability, boolean synthetic) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullability, new NameId(), synthetic);
    }

    private static CircuitBreaker requestBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new HierarchyCircuitBreakerService(CircuitBreakerMetrics.NOOP, settings, List.of(), clusterSettings).getBreaker(
            CircuitBreaker.REQUEST
        );
    }
}
