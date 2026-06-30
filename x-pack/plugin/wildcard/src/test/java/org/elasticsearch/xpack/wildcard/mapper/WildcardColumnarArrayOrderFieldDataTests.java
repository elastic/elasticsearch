/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.elasticsearch.index.mapper.AbstractColumnarArrayOrderFieldDataTestCase;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Collection;
import java.util.Collections;

/**
 * Fielddata round-trips for wildcard fields in strictly columnar mode, which store their values in document order with inline nulls
 * ({@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}) instead of sorting and deduplicating them.
 */
public class WildcardColumnarArrayOrderFieldDataTests extends AbstractColumnarArrayOrderFieldDataTestCase {

    @Override
    protected String fieldTypeName() {
        return "wildcard";
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singleton(new Wildcard());
    }
}
