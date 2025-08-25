/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringToIntMap;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.StringToIntSet;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenEvaluator;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringView;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.AndStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.AnyString;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.EqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.LengthStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.NotEqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.OrStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringSetConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringToIntMapConstraint;

public class SubTokenEvaluatorFactory {
    public static SubTokenEvaluator<SubstringView> from(int bitmask, StringConstraint stringConstraint) {
        return switch (stringConstraint) {
            case EqualsStringConstraint esc -> new SubTokenEvaluator<>(
                bitmask,
                (input) -> input != null && input.equals(new SubstringView(esc.targetValue())) ? 1 : -1
            );
            case NotEqualsStringConstraint nesc -> new SubTokenEvaluator<>(
                bitmask,
                (input) -> input != null && input.equals(new SubstringView(nesc.targetValue())) == false ? 1 : -1
            );
            case LengthStringConstraint lsc -> new SubTokenEvaluator<>(
                bitmask,
                (input) -> input != null && input.length() == lsc.requiredLength() ? 1 : -1
            );
            case StringSetConstraint ssc -> new SubTokenEvaluator<>(bitmask, new StringToIntSet(ssc.keys()));
            case StringToIntMapConstraint stmc -> new SubTokenEvaluator<>(bitmask, new SubstringToIntMap(stmc.map()));
            case AndStringConstraint asc -> from(bitmask, asc.first()).and(from(bitmask, asc.second()));
            case OrStringConstraint osc -> from(bitmask, osc.first()).or(from(bitmask, osc.second()));
            case AnyString anyString -> new SubTokenEvaluator<>(bitmask, (input) -> input != null ? 1 : -1);
            default -> throw new IllegalArgumentException(
                "Unsupported StringConstraint type: " + stringConstraint.getClass().getSimpleName()
            );
        };
    }
}
