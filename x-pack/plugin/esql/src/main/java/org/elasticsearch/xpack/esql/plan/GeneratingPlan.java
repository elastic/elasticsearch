/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NameId;

import java.util.ArrayList;
import java.util.List;

public interface GeneratingPlan<PlanType extends GeneratingPlan<PlanType>> {
    List<Attribute> generatedAttributes();

    PlanType withGeneratedNames(List<String> newNames);

    static List<Alias> renameAliases(List<Alias> originalAliases, List<String> newNames) {
        if (newNames.size() != originalAliases.size()) {
            throw new IllegalArgumentException(
                "Number of new names is [" + newNames.size() + "] but there are [" + originalAliases.size() + "] names."
            );
        }

        List<Alias> newFields = new ArrayList<>(originalAliases.size());
        for (int i = 0; i < originalAliases.size(); i++) {
            Alias field = originalAliases.get(i);
            String newName = newNames.get(i);
            if (field.name().equals(newName)) {
                newFields.add(field);
            } else {
                newFields.add(new Alias(field.source(), newName, field.qualifier(), field.child(), new NameId(), field.synthetic()));
            }
        }

        return newFields;
    }
}
