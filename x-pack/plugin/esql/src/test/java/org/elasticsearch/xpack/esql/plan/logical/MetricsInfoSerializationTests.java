/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

/** Verifies that older peers can read the plan without the optional sampling hint. */
public class MetricsInfoSerializationTests extends AbstractLogicalPlanSerializationTests<MetricsInfo> {
    @Override
    protected MetricsInfo createTestInstance() {
        return new MetricsInfo(randomSource(), randomChild(0), randomBoolean() ? null : between(0, Integer.MAX_VALUE));
    }

    @Override
    protected MetricsInfo mutateInstance(MetricsInfo instance) {
        if (randomBoolean()) {
            return instance.replaceChild(randomValueOtherThan(instance.child(), () -> randomChild(0)));
        }
        return instance.withLimit(randomValueOtherThan(instance.limit(), () -> randomBoolean() ? null : between(0, Integer.MAX_VALUE)));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testOlderVersionOmitsLimit() throws IOException {
        MetricsInfo instance = new MetricsInfo(Source.EMPTY, randomChild(0), between(1, 1000));
        MetricsInfo copy = copyInstance(instance, TransportVersionUtils.getPreviousVersion(MetricsInfo.ESQL_METRICS_INFO_LIMIT));
        assertNull(copy.limit());
        assertEquals(instance.withLimit(null), copy);
    }
}
