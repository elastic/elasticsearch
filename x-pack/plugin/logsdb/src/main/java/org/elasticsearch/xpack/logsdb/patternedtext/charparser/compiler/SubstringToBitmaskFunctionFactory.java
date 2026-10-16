/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringToIntegerMap;
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

import java.util.function.ToIntFunction;

public class SubstringToBitmaskFunctionFactory {
    public static ToIntFunction<SubstringView> from(int bitmask, StringConstraint stringConstraint) {
        return switch (stringConstraint) {
            case StringToIntMapConstraint constraint -> SubstringToIntegerMap.builder().addAll(constraint.map().keySet(), bitmask).build();
            case StringSetConstraint constraint -> SubstringToIntegerMap.builder().addAll(constraint.keys(), bitmask).build();
            case EqualsStringConstraint constraint -> new ToIntFunction<>() {
                private final SubstringView target = new SubstringView(constraint.targetValue());

                @Override
                public int applyAsInt(SubstringView input) {
                    return target.equals(input) ? bitmask : 0;
                }
            };
            case NotEqualsStringConstraint constraint -> new ToIntFunction<>() {
                private final SubstringView target = new SubstringView(constraint.targetValue());

                @Override
                public int applyAsInt(SubstringView input) {
                    return target.equals(input) ? 0 : bitmask;
                }
            };
            case LengthStringConstraint constraint -> input -> input.length() == constraint.requiredLength() ? bitmask : 0;
            case AnyString ignored -> input -> bitmask;
            case OrStringConstraint orConstraint -> SubstringToBitmaskChain.builder()
                .add(orConstraint.first(), bitmask)
                .add(orConstraint.second(), bitmask)
                .build();
            case AndStringConstraint andConstraint -> and(from(bitmask, andConstraint.first()), from(bitmask, andConstraint.second()));
            default -> throw new IllegalArgumentException(
                "Unsupported StringConstraint type: " + stringConstraint.getClass().getSimpleName()
            );
        };

    }

    private static ToIntFunction<SubstringView> and(final ToIntFunction<SubstringView> first, final ToIntFunction<SubstringView> second) {
        return input -> first.applyAsInt(input) & second.applyAsInt(input);
    }
}
