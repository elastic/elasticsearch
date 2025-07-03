/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public abstract class TupleAbstractBlockSourceOperator<T, S> extends AbstractBlockSourceOperator {
    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<Tuple<T, S>> values;
    private final ElementType firstElementType;
    private final ElementType secondElementType;

    public TupleAbstractBlockSourceOperator(
        BlockFactory blockFactory,
        List<Tuple<T, S>> values,
        ElementType firstElementType,
        ElementType secondElementType
    ) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS, firstElementType, secondElementType);
    }

    public TupleAbstractBlockSourceOperator(
        BlockFactory blockFactory,
        List<Tuple<T, S>> values,
        int maxPagePositions,
        ElementType firstElementType,
        ElementType secondElementType
    ) {
        super(blockFactory, maxPagePositions);
        this.values = values;
        this.firstElementType = firstElementType;
        this.secondElementType = secondElementType;
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        try (var blockBuilder1 = firstElementBlockBuilder(length); var blockBuilder2 = secondElementBlockBuilder(length)) {
            for (int i = 0; i < length; i++) {
                Tuple<T, S> item = values.get(positionOffset + i);
                if (item.v1() == null) {
                    blockBuilder1.appendNull();
                } else {
                    consumeFirstElement(item.v1(), blockBuilder1);
                }
                if (item.v2() == null) {
                    blockBuilder2.appendNull();
                } else {
                    consumeSecondElement(item.v2(), blockBuilder2);
                }
            }
            currentPosition += length;
            return new Page(Block.Builder.buildAll(blockBuilder1, blockBuilder2));
        }
    }

    protected abstract void consumeFirstElement(T t, Block.Builder blockBuilder1);

    protected Block.Builder firstElementBlockBuilder(int length) {
        return firstElementType.newBlockBuilder(length, blockFactory);
    }

    protected Block.Builder secondElementBlockBuilder(int length) {
        return secondElementType.newBlockBuilder(length, blockFactory);
    }

    protected abstract void consumeSecondElement(S t, Block.Builder blockBuilder1);

    @Override
    protected int remaining() {
        return values.size() - currentPosition;
    }

    public List<ElementType> elementTypes() {
        return List.of(firstElementType, secondElementType);
    }

    public List<Tuple<T, S>> values() {
        return values;
    }
}
