/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomMinimumVersion;
import static org.hamcrest.Matchers.equalTo;

public class LogicalOptimizerContextTests extends ESTestCase {
    public void testToString() {
        // Random looking numbers for FoldContext are indeed random. Just so we have consistent numbers to assert on in toString.
        // Same for the transport version.
        LogicalOptimizerContext ctx = logicalOptimizerContext(
            EsqlTestUtils.TEST_CFG,
            new FoldContext(102),
            FieldAttribute.ESQL_FIELD_ATTRIBUTE_DROP_TYPE
        );
        ctx.foldCtx().trackAllocation(Source.EMPTY, 99);
        assertThat(
            ctx.toString(),
            equalTo(
                "LogicalOptimizerContext[configuration="
                    + EsqlTestUtils.TEST_CFG
                    + ", foldCtx=FoldContext[3/102], minimumVersion=9075000, flags="
                    + EsqlFlags.DEFAULTS
                    + "]"
            )
        );
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(randomLogicalOptimizerContext(), this::copy, this::mutate);
    }

    private LogicalOptimizerContext randomLogicalOptimizerContext() {
        return new LogicalOptimizerContext(
            ConfigurationTestUtils.randomConfiguration(),
            randomFoldContext(),
            randomMinimumVersion(),
            randomFlags()
        );
    }

    private LogicalOptimizerContext copy(LogicalOptimizerContext c) {
        return new LogicalOptimizerContext(c.configuration(), c.foldCtx(), c.minimumVersion(), c.flags());
    }

    private LogicalOptimizerContext mutate(LogicalOptimizerContext c) {
        Configuration configuration = c.configuration();
        FoldContext foldCtx = c.foldCtx();
        TransportVersion minVersion = c.minimumVersion();
        EsqlFlags flags = c.flags();
        switch (randomIntBetween(0, 3)) {
            case 0 -> configuration = randomValueOtherThan(configuration, ConfigurationTestUtils::randomConfiguration);
            case 1 -> foldCtx = randomValueOtherThan(foldCtx, this::randomFoldContext);
            case 2 -> minVersion = randomValueOtherThan(minVersion, EsqlTestUtils::randomMinimumVersion);
            case 3 -> flags = randomValueOtherThan(flags, this::randomFlags);
            default -> throw new AssertionError("unexpected mutation branch");
        }
        return new LogicalOptimizerContext(configuration, foldCtx, minVersion, flags);
    }

    private FoldContext randomFoldContext() {
        FoldContext ctx = new FoldContext(randomNonNegativeLong());
        if (randomBoolean()) {
            ctx.trackAllocation(Source.EMPTY, randomLongBetween(0, ctx.initialAllowedBytes()));
        }
        return ctx;
    }

    private EsqlFlags randomFlags() {
        return new EsqlFlags(randomBoolean(), randomIntBetween(-1, 127), randomBoolean(), randomIntBetween(1, 50), randomIntBetween(1, 20));
    }
}
