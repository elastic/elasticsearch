/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.common.xcontent.support.filtering;

import org.elasticsearch.core.Glob;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FilterPath {

    static final FilterPath EMPTY = new FilterPath();

    private final String filter;
    private final String segment;
    private final FilterPath next;
    private final boolean simpleWildcard;
    private final boolean doubleWildcard;

    protected FilterPath(String filter, String segment, FilterPath next) {
        this.filter = filter;
        this.segment = segment;
        this.next = next;
        this.simpleWildcard = (segment != null) && (segment.length() == 1) && (segment.charAt(0) == '*');
        this.doubleWildcard = (segment != null) && (segment.length() == 2) && (segment.charAt(0) == '*') && (segment.charAt(1) == '*');
    }

    private FilterPath() {
        this("<empty>", "", null);
    }

    public FilterPath matchProperty(String name) {
        if ((next != null) && (simpleWildcard || doubleWildcard || Glob.globMatch(segment, name))) {
            return next;
        }
        return null;
    }

    public boolean matches() {
        return next == null;
    }

    boolean isDoubleWildcard() {
        return doubleWildcard;
    }

    boolean isSimpleWildcard() {
        return simpleWildcard;
    }

    String getSegment() {
        return segment;
    }

    FilterPath getNext() {
        return next;
    }

    public static FilterPath[] compile(Set<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        List<FilterPath> paths = new ArrayList<>();
        for (String filter : filters) {
            if (filter != null) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    paths.add(parse(filter, filter));
                }
            }
        }
        return paths.toArray(new FilterPath[paths.size()]);
    }

    private static FilterPath parse(final String filter, final String segment) {
        int end = segment.length();

        for (int i = 0; i < end; ) {
            char c = segment.charAt(i);

            if (c == '.') {
                String current = segment.substring(0, i).replaceAll("\\\\.", ".");
                return new FilterPath(filter, current, parse(filter, segment.substring(i + 1)));
            }
            ++i;
            if ((c == '\\') && (i < end) && (segment.charAt(i) == '.')) {
                ++i;
            }
        }
        return new FilterPath(filter, segment.replaceAll("\\\\.", "."), EMPTY);
    }

    @Override
    public String toString() {
        return "FilterPath [filter=" + filter + ", segment=" + segment + "]";
    }
}
