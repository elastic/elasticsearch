/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.sql.session.AbstractRowSet;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

abstract class ResultRowSet<E extends NamedWriteable> extends AbstractRowSet {

    private final List<E> extractors;
    private final List<Integer> mask;

    ResultRowSet(List<E> extractors, List<Integer> mask) {
        this.extractors = extractors;
        this.mask = mask;
        Check.isTrue(mask.size() <= extractors.size(), "Invalid number of extracted columns specified");
    }

    @Override
    public final int columnCount() {
        return mask.size();
    }

    @Override
    protected Object getColumn(int column) {
        return extractValue(userExtractor(column));
    }

    List<E> extractors() {
        return extractors;
    }

    List<Integer> mask() {
        return mask;
    }

    E userExtractor(int column) {
        return extractors.get(mask.get(column));
    }

    Object resultColumn(int column) {
        return extractValue(extractors().get(column));
    }

    int resultColumnCount() {
        return extractors.size();
    }

    void forEachResultColumn(Consumer<? super Object> action) {
        Objects.requireNonNull(action);
        int rowSize = resultColumnCount();
        for (int i = 0; i < rowSize; i++) {
            action.accept(resultColumn(i));
        }
    }

    protected abstract Object extractValue(E e);
}
