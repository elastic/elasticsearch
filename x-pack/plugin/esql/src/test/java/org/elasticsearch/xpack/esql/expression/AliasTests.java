/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;

import java.io.IOException;

public class AliasTests extends AbstractNamedExpressionSerializationTests<Alias> {
    public static Alias randomAlias() {
        Source source = SourceTests.randomSource();
        String name = randomAlphaOfLength(5);
        // TODO better randomChild
        Expression child = ReferenceAttributeTests.randomReferenceAttribute(false);
        boolean synthetic = randomBoolean();
        return new Alias(source, name, child, new NameId(), synthetic);
    }

    @Override
    protected Alias createTestInstance() {
        return randomAlias();
    }

    @Override
    protected Alias mutateInstance(Alias instance) throws IOException {
        Source source = instance.source();
        String name = instance.name();
        Expression child = instance.child();
        boolean synthetic = instance.synthetic();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> child = randomValueOtherThan(child, () -> ReferenceAttributeTests.randomReferenceAttribute(false));
            case 2 -> synthetic = false == synthetic;
        }
        return new Alias(source, name, child, instance.id(), synthetic);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    @Override
    protected Alias mutateNameId(Alias instance) {
        return instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }
}
