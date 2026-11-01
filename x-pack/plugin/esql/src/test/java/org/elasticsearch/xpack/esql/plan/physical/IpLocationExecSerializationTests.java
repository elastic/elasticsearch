/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.List;

public class IpLocationExecSerializationTests extends CompoundOutputEvalExecSerializationTests {

    @Override
    protected CompoundOutputEvalExec createInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        return new IpLocationExec(
            source,
            child,
            input,
            outputFieldNames,
            outputFieldAttributes,
            randomAlphaOfLengthBetween(3, 10) + ".mmdb",
            randomBoolean()
        );
    }

    @Override
    protected CompoundOutputEvalExec mutateInstance(CompoundOutputEvalExec instance) throws IOException {
        IpLocationExec ip = (IpLocationExec) instance;
        PhysicalPlan child = ip.child();
        Expression input = ip.input();
        List<String> outputFieldNames = ip.outputFieldNames();
        List<Attribute> outputFieldAttributes = ip.outputFieldAttributes();
        String databaseFile = ip.databaseFile();
        boolean firstOnly = ip.firstOnly();

        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> input = randomValueOtherThan(input, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 2 -> {
                final int nameSize = outputFieldNames.size();
                outputFieldNames = randomValueOtherThan(
                    outputFieldNames,
                    () -> randomList(nameSize, nameSize, () -> randomAlphaOfLengthBetween(1, 10))
                );
            }
            case 3 -> {
                final int attrSize = outputFieldAttributes.size();
                outputFieldAttributes = randomValueOtherThan(outputFieldAttributes, () -> randomFieldAttributes(attrSize, attrSize, false));
            }
            case 4 -> databaseFile = randomValueOtherThan(databaseFile, () -> randomAlphaOfLengthBetween(3, 10) + ".mmdb");
            case 5 -> firstOnly = firstOnly == false;
        }
        return new IpLocationExec(ip.source(), child, input, outputFieldNames, outputFieldAttributes, databaseFile, firstOnly);
    }
}
