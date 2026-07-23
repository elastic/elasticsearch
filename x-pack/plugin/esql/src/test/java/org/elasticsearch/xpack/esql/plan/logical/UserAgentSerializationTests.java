/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.command.UserAgentFunctionBridge;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.SequencedMap;

import static org.elasticsearch.xpack.esql.expression.function.FieldAttributeTestUtils.createFieldAttribute;

public class UserAgentSerializationTests extends CompoundOutputEvalSerializationTests<UserAgent> {

    @Override
    protected UserAgent createInitialInstance(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix) {
        boolean extractDeviceType = randomBoolean();
        String regexFile = randomAlphaOfLengthBetween(3, 10);
        SequencedMap<String, Class<?>> filteredFields = randomFilteredFields(extractDeviceType);
        return UserAgent.createInitialInstance(
            source,
            child,
            input,
            Objects.requireNonNull(outputFieldPrefix),
            extractDeviceType,
            regexFile,
            filteredFields
        );
    }

    @Override
    protected UserAgent mutateInstance(UserAgent instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression input = instance.getInput();
        List<String> outputFieldNames = instance.outputFieldNames();
        List<Attribute> outputFieldAttributes = instance.generatedAttributes();
        boolean extractDeviceType = instance.extractDeviceType();
        String regexFile = instance.regexFile();

        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> input = randomValueOtherThan(input, () -> createFieldAttribute(0, false));
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
        return new UserAgent(instance.source(), child, input, outputFieldNames, outputFieldAttributes, extractDeviceType, regexFile);
    }

    private SequencedMap<String, Class<?>> randomFilteredFields(boolean extractDeviceType) {
        SequencedMap<String, Class<?>> allFields = UserAgentFunctionBridge.getAllOutputFields();
        LinkedHashMap<String, Class<?>> filtered = new LinkedHashMap<>();
        for (var entry : allFields.entrySet()) {
            if (entry.getKey().equals("device.type") && extractDeviceType == false) {
                continue;
            }
            if (randomBoolean() || filtered.isEmpty()) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return filtered;
    }
}
