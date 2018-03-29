/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.common.xcontent.support.filtering;

import org.elasticsearch.common.Glob;

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
