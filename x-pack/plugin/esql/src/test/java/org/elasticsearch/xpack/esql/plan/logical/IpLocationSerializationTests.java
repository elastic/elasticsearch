/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.SequencedMap;

public class IpLocationSerializationTests extends CompoundOutputEvalSerializationTests<IpLocation> {

    @Override
    protected IpLocation createInitialInstance(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix) {
        String databaseFile = randomAlphaOfLengthBetween(3, 10) + ".mmdb";
        boolean firstOnly = randomBoolean();
        SequencedMap<String, Class<?>> filteredFields = randomFilteredFields();
        return IpLocation.createInitialInstance(
            source,
            child,
            input,
            Objects.requireNonNull(outputFieldPrefix),
            databaseFile,
            firstOnly,
            filteredFields
        );
    }

    @Override
    protected IpLocation mutateInstance(IpLocation instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression input = instance.getInput();
        List<String> outputFieldNames = instance.outputFieldNames();
        List<Attribute> outputFieldAttributes = instance.generatedAttributes();
        String databaseFile = instance.databaseFile();
        boolean firstOnly = instance.firstOnly();

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
        return new IpLocation(instance.source(), child, input, outputFieldNames, outputFieldAttributes, databaseFile, firstOnly);
    }

    private SequencedMap<String, Class<?>> randomFilteredFields() {
        DatabaseProperty[] candidates = {
            DatabaseProperty.COUNTRY_ISO_CODE,
            DatabaseProperty.COUNTRY_NAME,
            DatabaseProperty.CITY_NAME,
            DatabaseProperty.LOCATION,
            DatabaseProperty.ASN,
            DatabaseProperty.ANONYMOUS_VPN,
            DatabaseProperty.ACCURACY_RADIUS };
        LinkedHashMap<String, Class<?>> filtered = new LinkedHashMap<>();
        for (DatabaseProperty dp : candidates) {
            if (randomBoolean() || filtered.isEmpty()) {
                filtered.put(dp.fieldName(), dp.fieldType());
            }
        }
        return filtered;
    }
}
