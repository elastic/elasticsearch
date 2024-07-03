/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;

import java.util.ArrayList;
import java.util.List;

public interface GeneratingPlan<PlanType extends GeneratingPlan<PlanType>> {
    List<Attribute> generatedAttributes();

    PlanType withGeneratedNames(List<String> newNames);

    static List<Alias> renameAliases(List<Alias> originalAliases, List<String> newNames) {
        if (newNames.size() != originalAliases.size()) {
            throw new IllegalArgumentException(
                "Number of new names is [" + newNames.size() + "] but there are [" + originalAliases.size() + "] existing names."
            );
        }

        AttributeMap.Builder<Attribute> aliasReplacedByBuilder = AttributeMap.builder();
        List<Alias> newFields = new ArrayList<>(originalAliases.size());
        for (int i = 0; i < originalAliases.size(); i++) {
            Alias field = originalAliases.get(i);
            String newName = newNames.get(i);
            if (field.name().equals(newName)) {
                newFields.add(field);
            } else {
                Alias newField = new Alias(field.source(), newName, field.qualifier(), field.child(), new NameId(), field.synthetic());
                newFields.add(newField);
                aliasReplacedByBuilder.put(field.toAttribute(), newField.toAttribute());
            }
        }
        AttributeMap<Attribute> aliasReplacedBy = aliasReplacedByBuilder.build();

        // We need to also update any references to the old attributes in the new attributes; e.g.
        // EVAL x = 1, y = x + 1
        // renaming x, y to x1, y1
        // so far became
        // EVAL x1 = 1, y1 = x + 1
        // - but x doesn't exist anymore, so replace it by x1 to obtain
        // EVAL x1 = 1, y1 = x1 + 1

        List<Alias> newFieldsWithUpdatedRefs = new ArrayList<>(originalAliases.size());
        for (Alias newField : newFields) {
            newFieldsWithUpdatedRefs.add((Alias) newField.transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r)));
        }

        return newFieldsWithUpdatedRefs;
    }
}
