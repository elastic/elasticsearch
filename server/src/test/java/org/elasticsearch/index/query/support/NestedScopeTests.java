/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class NestedScopeTests extends ESTestCase {

    public void testUnwindToAncestorLevel() {
        NestedScope nestedScope = new NestedScope();
        NestedObjectMapper outer = buildNestedMapper("a");
        NestedObjectMapper inner = buildNestedMapper("a.b");
        nestedScope.nextLevel(outer);
        nestedScope.nextLevel(inner);

        try (Releasable ignored = nestedScope.unwindTo(outer)) {
            assertThat(nestedScope.getObjectMapper(), sameInstance(outer));
        }
        assertThat(nestedScope.getObjectMapper(), sameInstance(inner));
    }

    public void testUnwindToRoot() {
        NestedScope nestedScope = new NestedScope();
        NestedObjectMapper outer = buildNestedMapper("a");
        NestedObjectMapper inner = buildNestedMapper("a.b");
        nestedScope.nextLevel(outer);
        nestedScope.nextLevel(inner);

        try (Releasable ignored = nestedScope.unwindTo(null)) {
            assertThat(nestedScope.getObjectMapper(), nullValue());
        }
        assertThat(nestedScope.getObjectMapper(), sameInstance(inner));
        nestedScope.previousLevel();
        assertThat(nestedScope.getObjectMapper(), sameInstance(outer));
    }

    public void testUnwindToCurrentLevelIsANoOp() {
        NestedScope nestedScope = new NestedScope();
        NestedObjectMapper level = buildNestedMapper("a");
        nestedScope.nextLevel(level);

        try (Releasable ignored = nestedScope.unwindTo(level)) {
            assertThat(nestedScope.getObjectMapper(), sameInstance(level));
        }
        assertThat(nestedScope.getObjectMapper(), sameInstance(level));
    }

    private static NestedObjectMapper buildNestedMapper(String path) {
        return new NestedObjectMapper.Builder(path, IndexVersion.current(), query -> { throw new UnsupportedOperationException(); }, null)
            .build(MapperBuilderContext.root(false, false));
    }
}
