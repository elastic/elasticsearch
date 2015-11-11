/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.util.Arrays;

import static org.elasticsearch.watcher.support.Exceptions.illegalState;


/**
 * The only true way today to compare search request object (outside of core) is to
 * serialize it and compare the serialized output. this is heavy obviously, but luckily we
 * don't compare search requests in normal runtime... we only do it in the tests. The is here basically
 * due to the lack of equals/hashcode support in SearchRequest in core.
 */
public final class SearchRequestEquivalence {

    public static final SearchRequestEquivalence INSTANCE = new SearchRequestEquivalence();

    private SearchRequestEquivalence() {
    }

    public final boolean equivalent(@Nullable SearchRequest a, @Nullable SearchRequest b) {
        return a == b ? true : (a != null && b != null ? this.doEquivalent(a, b) : false);
    }

    protected boolean doEquivalent(SearchRequest r1, SearchRequest r2) {
        try {
            BytesStreamOutput output1 = new BytesStreamOutput();
            r1.writeTo(output1);
            byte[] bytes1 = output1.bytes().toBytes();
            output1.reset();
            r2.writeTo(output1);
            byte[] bytes2 = output1.bytes().toBytes();
            return Arrays.equals(bytes1, bytes2);
        } catch (Throwable t) {
            throw illegalState("could not compare search requests", t);
        }
    }
}
