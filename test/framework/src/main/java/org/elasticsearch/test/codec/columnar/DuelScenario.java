/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import java.util.List;
import java.util.function.Supplier;

/**
 * A named corpus shape for a duel. A scenario owns document structure only; the literals come from the value
 * generators of the field type under test. Document counts and per-document choices are randomized through
 * {@link org.elasticsearch.test.ESTestCase}, so a corpus reproduces from a test seed while the shape stays fixed.
 *
 * @param name       the scenario name, included in failure messages
 * @param builder    generates a fresh corpus on every call
 * @param multiValue whether the fields are mapped to accept several values a document. A scenario that says not maps
 *                   them {@code doc_values.multi_value: false}, which selects a different doc-values layout, and
 *                   generates at most one value a document.
 */
public record DuelScenario(String name, Supplier<List<DuelDoc>> builder, boolean multiValue) {

    public DuelScenario(final String name, final Supplier<List<DuelDoc>> builder) {
        this(name, builder, true);
    }

    /**
     * @return a freshly generated corpus for this shape. Called once per duel so every index in a pair sees the
     *         identical document list.
     */
    public List<DuelDoc> documents() {
        return builder.get();
    }
}
