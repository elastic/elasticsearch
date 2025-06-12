/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;

/**
 * A list of wildcard patterns. Each pattern is a {@link WildcardPattern} that can be used to match strings and is
 * similar to basic regex, supporting '?' wildcard for single character (same as regex  ".")
 * and '*' wildcard for multiple characters (same as regex ".*")
 * <p>
 * Allows escaping based on a regular char
 *
 */
public class WildcardPatternList extends AbstractStringPattern implements NamedWriteable {
    public static final Entry ENTRY = new Entry(WildcardPatternList.class, "WildcardPatternList", WildcardPatternList::new);
    public static final String NAME = "WildcardPatternList";
    private final List<WildcardPattern> patternList;

    public WildcardPatternList(List<WildcardPattern> patterns) {
        this.patternList = patterns;
    }

    public WildcardPatternList(StreamInput in) throws IOException {
        this(in.readCollectionAsList(WildcardPattern::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(patternList, (o, pattern) -> pattern.writeTo(o));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static WildcardPatternList readFrom(StreamInput in) throws IOException {
        return new WildcardPatternList(in.readCollectionAsList(WildcardPattern::readFrom));
    }

    public List<WildcardPattern> patternList() {
        return patternList;
    }

    @Override
    public Automaton createAutomaton(boolean ignoreCase) {
        List<Automaton> automatonList = patternList.stream().map(x -> x.createAutomaton(ignoreCase)).toList();
        Automaton result = Operations.union(automatonList);
        return Operations.determinize(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    @Override
    public String asJavaRegex() {
        return patternList.stream().map(WildcardPattern::asJavaRegex).collect(Collectors.joining("|"));
    }

    @Override
    public String pattern() {
        if (patternList.isEmpty()) {
            return "";
        }
        if (patternList.size() == 1) {
            return patternList.getFirst().pattern();
        }
        return "(\"" + patternList.stream().map(WildcardPattern::pattern).collect(Collectors.joining("\", \"")) + "\")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(patternList);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        WildcardPatternList other = (WildcardPatternList) obj;
        return Objects.equals(patternList, other.patternList);
    }

}
