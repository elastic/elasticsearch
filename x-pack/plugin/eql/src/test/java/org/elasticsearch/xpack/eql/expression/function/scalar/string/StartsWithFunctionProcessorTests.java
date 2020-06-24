/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.eql.EqlTestUtils.randomConfigurationWithCaseSensitive;

public class StartsWithFunctionProcessorTests extends org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWithProcessorTests{

    @Override
    protected Supplier<Boolean> isCaseSensitiveGenerator() {
        return () -> randomBoolean();
    }

    @Override
    protected Supplier<Configuration> configurationGenerator() {
        return () -> randomConfigurationWithCaseSensitive(isCaseSensitive);
    }

    @Override
    protected Supplier<StartsWith> startsWithInstantiator(Source source, Expression field, Expression pattern) {
        return () -> new org.elasticsearch.xpack.eql.expression.function.scalar.string.StartsWith(source, field, pattern, config);
    }

}