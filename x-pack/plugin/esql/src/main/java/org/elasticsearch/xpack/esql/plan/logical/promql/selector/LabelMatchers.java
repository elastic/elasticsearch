/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.common.util.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Immutable collection of label matchers for a PromQL selector.
 */
public class LabelMatchers {
    /**
     * Empty label matchers for literal selectors and other cases with no label constraints.
     */
    public static final LabelMatchers EMPTY = new LabelMatchers(emptyList());

    private final List<LabelMatcher> labelMatchers;
    private final Map<String, LabelMatcher> nameToMatcher;

    public LabelMatchers(List<LabelMatcher> labelMatchers) {
        Objects.requireNonNull(labelMatchers, "label matchers cannot be null");
        this.labelMatchers = labelMatchers;
        int size = labelMatchers.size();
        if (size == 0) {
            nameToMatcher = emptyMap();
        } else {
            nameToMatcher = Maps.newLinkedHashMapWithExpectedSize(size);
            for (LabelMatcher lm : labelMatchers) {
                nameToMatcher.put(lm.name(), lm);
            }
        }
    }

    public List<LabelMatcher> matchers() {
        return labelMatchers;
    }

    public LabelMatcher nameLabel() {
        return nameToMatcher.get(LabelMatcher.NAME);
    }

    public boolean isEmpty() {
        return labelMatchers.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelMatchers);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LabelMatchers other = (LabelMatchers) obj;
        return Objects.equals(labelMatchers, other.labelMatchers);
    }

    @Override
    public String toString() {
        return labelMatchers.toString();
    }
}
