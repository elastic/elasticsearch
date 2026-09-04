/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.mapper.AbstractColumnarArrayOrderSyntheticSourceTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

/**
 * Round-trips synthetic {@code _source} for high-cardinality match_only_text fields in strictly columnar mode, which store their values in
 * document order with inline nulls ({@code ArrayOrderInlineNull}) instead of a sidecar {@code .offsets} field.
 */
public class MatchOnlyTextColumnarArrayOrderSyntheticSourceTests extends AbstractColumnarArrayOrderSyntheticSourceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected String fieldTypeName() {
        return "match_only_text";
    }
}
