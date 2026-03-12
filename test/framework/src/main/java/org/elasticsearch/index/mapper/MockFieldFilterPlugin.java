/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.function.Function;

public class MockFieldFilterPlugin extends Plugin implements MapperPlugin {

    @Override
    public Function<String, FieldPredicate> getFieldFilter() {
        // this filter doesn't filter any field out, but it's used to exercise the code path executed when the filter is not no-op
        return index -> new FieldPredicate() {
            @Override
            public boolean test(String field) {
                return true;
            }

            @Override
            public String modifyHash(String hash) {
                return hash + ":includeall";
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }
        };
    }
}
