/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;

public class VectorMatch {

    public enum Filter {
        IGNORING,
        ON,
        NONE
    }

    public enum Joining {
        LEFT,
        RIGHT,
        NONE
    }

    public static final VectorMatch NONE = new VectorMatch(Filter.NONE, emptySet(), Joining.NONE, emptySet());

    private final Filter filter;
    private final Set<String> filterLabels;

    private final Joining joining;
    private final Set<String> groupingLabels;

    public VectorMatch(Filter filter, Set<String> filterLabels, Joining joining, Set<String> groupingLabels) {
        this.filter = filter;
        this.filterLabels = filterLabels;
        this.joining = joining;
        this.groupingLabels = groupingLabels;
    }

    public Filter filter() {
        return filter;
    }

    public Set<String> filterLabels() {
        return filterLabels;
    }

    public Joining grouping() {
        return joining;
    }

    public Set<String> groupingLabels() {
        return groupingLabels;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            VectorMatch that = (VectorMatch) o;
            return filter == that.filter
                && Objects.equals(filterLabels, that.filterLabels)
                && joining == that.joining
                && Objects.equals(groupingLabels, that.groupingLabels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, filterLabels, joining, groupingLabels);
    }

    @Override
    public String toString() {
        String filterString = filter != Filter.NONE ? filter.name().toLowerCase(Locale.ROOT) + "(" + filterLabels + ")" : EMPTY;
        String groupingString = joining != Joining.NONE
            ? " " + joining.name().toLowerCase(Locale.ROOT) + (groupingLabels.isEmpty() == false ? "(" + groupingLabels + ")" : EMPTY) + " "
            : EMPTY;
        return filterString + groupingString;
    }
}
