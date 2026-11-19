/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IntoSerializationTests extends AbstractLogicalPlanSerializationTests<Into> {
    @Override
    protected Into createTestInstance() {
        return new Into(randomSource(), randomChild(0), randomAlphaOfLength(10));
    }

    public void testOutputRegeneratedOnRead() throws IOException {
        Into original = createTestInstance();
        Into roundTripped = copyInstance(original, TransportVersion.current());
        assertThat(roundTripped.output().stream().map(Attribute::name).toList(), equalTo(List.of("total", "created")));
        assertThat(roundTripped.output().stream().map(Attribute::dataType).toList(), equalTo(List.of(DataType.LONG, DataType.LONG)));
        // Regenerated attributes have fresh NameIds; if output were serialized, the original ids would be preserved.
        assertThat(roundTripped.output().get(0).id(), not(equalTo(original.output().get(0).id())));
    }

    @Override
    protected Into mutateInstance(Into instance) throws IOException {
        LogicalPlan child = instance.child();
        String destination = instance.destination();
        switch (between(0, 1)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> destination = randomValueOtherThan(destination, () -> randomAlphaOfLength(10));
            default -> throw new AssertionError("Unexpected case");
        }
        return new Into(instance.source(), child, destination);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
