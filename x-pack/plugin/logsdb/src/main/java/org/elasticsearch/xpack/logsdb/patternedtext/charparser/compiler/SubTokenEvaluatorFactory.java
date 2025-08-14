/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.StringToIntMap;
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
        if (stringConstraint instanceof EqualsStringConstraint) {
            EqualsStringConstraint esc = (EqualsStringConstraint) stringConstraint;
            return new SubTokenEvaluator<>(
                bitmask,
                (input) -> input != null && input.equals(new SubstringView(esc.targetValue())) ? 1 : -1
            );
        } else if (stringConstraint instanceof NotEqualsStringConstraint) {
            NotEqualsStringConstraint nesc = (NotEqualsStringConstraint) stringConstraint;
            return new SubTokenEvaluator<>(
                bitmask,
                (input) -> input != null && input.equals(new SubstringView(nesc.targetValue())) == false ? 1 : -1
            );
        } else if (stringConstraint instanceof LengthStringConstraint) {
            LengthStringConstraint lsc = (LengthStringConstraint) stringConstraint;
            return new SubTokenEvaluator<>(bitmask, (input) -> input != null && input.length() == lsc.requiredLength() ? 1 : -1);
        } else if (stringConstraint instanceof StringSetConstraint) {
            StringSetConstraint ssc = (StringSetConstraint) stringConstraint;
            return new SubTokenEvaluator<>(bitmask, new StringToIntSet(ssc.keys()));
        } else if (stringConstraint instanceof StringToIntMapConstraint) {
            StringToIntMapConstraint stmc = (StringToIntMapConstraint) stringConstraint;
            return new SubTokenEvaluator<>(bitmask, new StringToIntMap(stmc.map()));
        } else if (stringConstraint instanceof AndStringConstraint) {
            AndStringConstraint asc = (AndStringConstraint) stringConstraint;
            return from(bitmask, asc.first()).and(from(bitmask, asc.second()));
        } else if (stringConstraint instanceof OrStringConstraint) {
            OrStringConstraint osc = (OrStringConstraint) stringConstraint;
            return from(bitmask, osc.first()).or(from(bitmask, osc.second()));
        } else if (stringConstraint instanceof AnyString) {
            return new SubTokenEvaluator<>(bitmask, (input) -> input != null ? 1 : -1);
        } else {
            throw new IllegalArgumentException("Unsupported StringConstraint type: " + stringConstraint.getClass().getSimpleName());
        }
    }
}
