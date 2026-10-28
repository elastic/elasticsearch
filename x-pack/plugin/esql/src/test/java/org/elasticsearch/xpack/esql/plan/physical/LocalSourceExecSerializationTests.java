/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplierTests;

import java.io.IOException;
import java.util.List;

public class LocalSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LocalSourceExec> {
    public static LocalSourceExec randomLocalSourceExec() {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 9, false);
        LocalSupplier supplier = LocalSupplierTests.randomLocalSupplier();
        return new LocalSourceExec(source, output, supplier);
    }

    @Override
    protected LocalSourceExec createTestInstance() {
        return randomLocalSourceExec();
    }

    @Override
    protected LocalSourceExec mutateInstance(LocalSourceExec instance) throws IOException {
        List<Attribute> output = instance.output();
        LocalSupplier supplier = instance.supplier();
        if (randomBoolean()) {
            output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 9, false));
        } else {
            supplier = randomValueOtherThan(supplier, () -> LocalSupplierTests.randomLocalSupplier());
        }
        return new LocalSourceExec(instance.source(), output, supplier);
    }
}
