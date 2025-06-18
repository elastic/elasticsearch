/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RLikePatternList extends AbstractStringPattern implements Writeable {

    private final List<RLikePattern> patternList;

    public RLikePatternList(List<RLikePattern> patternList) {

        this.patternList = patternList;
    }

    public RLikePatternList(StreamInput in) throws IOException {
        this(in.readCollectionAsList(RLikePattern::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(patternList, (o, pattern) -> pattern.writeTo(o));
    }

    public List<RLikePattern> patternList() {
        return patternList;
    }

    /**
     * Creates an automaton that matches any of the patterns in the list.
     * We create a single automaton that is the union of all individual automatons to improve performance
     */
    @Override
    public Automaton createAutomaton(boolean ignoreCase) {
        List<Automaton> automatonList = patternList.stream().map(x -> x.createAutomaton(ignoreCase)).toList();
        Automaton result = Operations.union(automatonList);
        return Operations.determinize(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    /**
     * Returns a Java regex that matches any of the patterns in the list.
     * The patterns are joined with the '|' operator to create a single regex.
     */
    @Override
    public String asJavaRegex() {
        return patternList.stream().map(RLikePattern::asJavaRegex).collect(Collectors.joining("|"));
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

        RLikePatternList other = (RLikePatternList) obj;
        return patternList.equals(other.patternList);
    }

    /**
     * Returns a string that matches any of the patterns in the list.
     * The patterns are joined with the '|' operator to create a single regex string.
     */
    @Override
    public String pattern() {
        if (patternList.isEmpty()) {
            return "";
        }
        if (patternList.size() == 1) {
            return patternList.get(0).pattern();
        }
        return "(\"" + patternList.stream().map(RLikePattern::pattern).collect(Collectors.joining("\", \"")) + "\")";
    }
}
