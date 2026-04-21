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

public class UserAgentExecSerializationTests extends CompoundOutputEvalExecSerializationTests {

    @Override
    protected CompoundOutputEvalExec createInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        List<String> outputFieldNames,
        List<Attribute> outputFieldAttributes
    ) {
        return new UserAgentExec(
            source,
            child,
            input,
            outputFieldNames,
            outputFieldAttributes,
            randomBoolean(),
            randomAlphaOfLengthBetween(3, 10)
        );
    }

    @Override
    protected CompoundOutputEvalExec mutateInstance(CompoundOutputEvalExec instance) throws IOException {
        UserAgentExec ua = (UserAgentExec) instance;
        PhysicalPlan child = ua.child();
        Expression input = ua.input();
        List<String> outputFieldNames = ua.outputFieldNames();
        List<Attribute> outputFieldAttributes = ua.outputFieldAttributes();
        boolean extractDeviceType = ua.extractDeviceType();
        String regexFile = ua.regexFile();

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
            case 4 -> extractDeviceType = extractDeviceType == false;
            case 5 -> regexFile = randomValueOtherThan(regexFile, () -> randomAlphaOfLengthBetween(3, 10));
        }
        return new UserAgentExec(ua.source(), child, input, outputFieldNames, outputFieldAttributes, extractDeviceType, regexFile);
    }
}
