/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NamedExpressions {
    /**
     * Calculates the actual output of a command given the new attributes plus the existing inputs that are emitted as outputs
     * @param fields the fields added by the command
     * @param childOutput the command input that has to be propagated as output
     * @return
     */
    public static List<Attribute> mergeOutputAttributes(
        List<? extends NamedExpression> fields,
        List<? extends NamedExpression> childOutput
    ) {
        return Expressions.asAttributes(mergeOutputExpressions(fields, childOutput));
    }

    /**
     * Merges output expressions of a command given the new attributes plus the existing inputs that are emitted as outputs.
     * As a general rule, child output will come first in the list, followed by the new fields.
     * In case of name collisions, only the last entry is preserved (previous expressions with the same name are discarded)
     * and the new attributes have precedence over the child output.
     * Attributes with qualifiers do not allow for name collisions.
     * @param fields the fields added by the command
     * @param childOutput the command input that has to be propagated as output
     * @return the output of the command taking into account shadowing of unqualified fields
     */
    public static List<NamedExpression> mergeOutputExpressions(
        List<? extends NamedExpression> fields,
        List<? extends NamedExpression> childOutput
    ) {
        Map<String, Integer> lastPositions = Maps.newHashMapWithExpectedSize(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            NamedExpression field = fields.get(i);
            if (field.qualifier() == null) {
                lastPositions.put(field.name(), i);
            }
        }
        List<NamedExpression> output = new ArrayList<>(childOutput.size() + fields.size());
        for (NamedExpression childAttr : childOutput) {
            if (childAttr.qualifier() != null || lastPositions.containsKey(childAttr.name()) == false) {
                output.add(childAttr);
            }
        }
        // do not add unqualified duplicate fields multiple times, only last one matters as output
        for (int i = 0; i < fields.size(); i++) {
            NamedExpression field = fields.get(i);
            if (field.qualifier() != null || lastPositions.get(field.name()) == i) {
                output.add(field);
            }
        }
        return output;
    }
}
