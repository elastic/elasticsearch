/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

// OR operator (|)
public final class SetIntConstraint implements IntConstraint {
    private final int[] allowedValues;
    private final Set<Integer> allowedValuesSet;

    public SetIntConstraint(int[] allowedValues) {
        this.allowedValuesSet = Arrays.stream(allowedValues).boxed().collect(Collectors.toSet());
        this.allowedValues = allowedValues;
    }

    @Override
    public boolean isApplicable(int value) {
        return allowedValuesSet.contains(value);
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        ArrayList<IntConstraints.Range> ranges = new ArrayList<>();
        Arrays.stream(allowedValues).sorted().forEach(val -> ranges.add(new IntConstraints.Range(val, val)));
        return ranges.toArray(new IntConstraints.Range[0]);
    }
}
