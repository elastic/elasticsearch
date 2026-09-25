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

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

/**
 * A chain of {@code ToIntFunction<SubstringView>} that can be applied in sequence to produce a bitmask for a given substring.
 * Each function in the chain produces a bitmask that is ORed together to produce the final result.
 * This is a way of applying multiple {@code StringConstraint} evaluations on a given substring into a single bitmask.
 * In order to optimize the evaluation, any map/set based evaluations are combined into a single
 * {@link SubstringToIntegerMap} evaluation. In addition, any "any string" constraints are combined into a single bitmask that is always
 * applied.
 * Conceptually, each function in the chain represents the constraints related to a single sub-token from the schema. Some of these
 * may be composite constraints (AND/OR), in which case a single function may represent multiple inner evaluations. Technically, however,
 * this is only the case for AND constraints, as OR constraints are decomposed into multiple functions in the chain.
 */
public class SubstringToBitmaskChain implements ToIntFunction<SubstringView> {
    /**
     * Any string evaluation that matches a map/set pattern can be combined into a single map evaluation for efficiency.
     */
    private final SubstringToIntegerMap substringToBitmaskMap;

    /**
     * A chain of functions to apply in sequence to produce the combined bitmask.
     */
    private final ToIntFunction<SubstringView>[] chain;

    /**
     * A bitmask that matches all strings.
     */
    private final int anyStringBitmask;

    @SuppressWarnings("unchecked")
    private SubstringToBitmaskChain(
        List<ToIntFunction<SubstringView>> chainList,
        SubstringToIntegerMap substringToBitmaskMap,
        int anyStringBitmask
    ) {
        this.chain = chainList.toArray(ToIntFunction[]::new);
        this.substringToBitmaskMap = substringToBitmaskMap;
        this.anyStringBitmask = anyStringBitmask;
    }

    @Override
    public int applyAsInt(SubstringView value) {
        int result = substringToBitmaskMap != null ? substringToBitmaskMap.applyAsInt(value) : 0;
        // noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < chain.length; i++) {
            result |= chain[i].applyAsInt(value);
        }
        result |= anyStringBitmask;
        return result;
    }

    public static class Builder {
        private final List<ToIntFunction<SubstringView>> chainList;
        private final SubstringToIntegerMap.Builder substringToBitmaskMapBuilder;
        private int anyStringBitmask;

        private Builder() {
            this.chainList = new ArrayList<>();
            this.substringToBitmaskMapBuilder = SubstringToIntegerMap.builder();
            this.anyStringBitmask = 0;
        }

        /**
         * Add a StringConstraint with the associated bitmask to the chain being built.
         * The constraint can be a composite one (AND/OR), in which case it will be decomposed accordingly, in which case it is
         * important to distinguish two different steps of runtime evaluation:
         * <ul>
         *     <li>The evaluation of the compiled form of a composite constraint may include multiple inner evaluations, but should
         *     always result in a single bitmask. Nevertheless, in case of an OR composite constraint, the inner evaluations can be added
         *     directly to the chain, as opposed to AND composite constraints, which need to be wrapped in a function that ensures all inner
         *     evaluations are applied in an AND manner.</li>
         *     <li>The evaluation of the chain itself involves the application of multiple constraints, each producing a bitmask that is
         *     ORed together to produce the final result.</li>
         * </ul>
         * @param stringConstraint the StringConstraint to add, potentially a composite constraint
         * @param bitmask the bitmask associated with the constraint
         * @return this builder for method chaining
         */
        public Builder add(StringConstraint stringConstraint, int bitmask) {
            switch (stringConstraint) {
                case StringToIntMapConstraint constraint -> substringToBitmaskMapBuilder.addAll(constraint.map().keySet(), bitmask);
                case StringSetConstraint constraint -> substringToBitmaskMapBuilder.addAll(constraint.keys(), bitmask);
                case EqualsStringConstraint constraint -> substringToBitmaskMapBuilder.add(constraint.targetValue(), bitmask);
                case NotEqualsStringConstraint constraint -> chainList.add(SubstringToBitmaskFunctionFactory.from(bitmask, constraint));
                case LengthStringConstraint constraint -> chainList.add(SubstringToBitmaskFunctionFactory.from(bitmask, constraint));
                case AnyString ignored -> this.anyStringBitmask |= bitmask;
                case OrStringConstraint orConstraint -> add(orConstraint.first(), bitmask).add(orConstraint.second(), bitmask);
                case AndStringConstraint andConstraint -> chainList.add(SubstringToBitmaskFunctionFactory.from(bitmask, andConstraint));
                default -> throw new IllegalArgumentException(
                    "Unsupported StringConstraint type: " + stringConstraint.getClass().getSimpleName()
                );
            }
            return this;
        }

        /**
         * Build the SubstringToBitmaskChain instance. Try to optimize the result by avoiding unnecessary chaining.
         * @return the built SubstringToBitmaskChain instance
         */
        public ToIntFunction<SubstringView> build() {
            SubstringToIntegerMap map = substringToBitmaskMapBuilder.isEmpty() ? null : substringToBitmaskMapBuilder.build();
            if (chainList.isEmpty() && anyStringBitmask == 0) {
                return map;
            }
            if (map == null && anyStringBitmask == 0 && chainList.size() == 1) {
                return chainList.getFirst();
            }
            if (map == null && chainList.isEmpty()) {
                return input -> anyStringBitmask;
            }
            return new SubstringToBitmaskChain(chainList, map, anyStringBitmask);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
