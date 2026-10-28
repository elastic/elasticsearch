/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class UnresolvedStarTests extends AbstractNamedExpressionSerializationTests<UnresolvedStar> {
    @Override
    protected UnresolvedStar createTestInstance() {
        return new UnresolvedStar(Source.EMPTY, randomBoolean() ? null : new UnresolvedAttribute(Source.EMPTY, randomAlphaOfLength(3)));
    }

    @Override
    protected UnresolvedStar mutateInstance(UnresolvedStar instance) {
        Source source = instance.source();
        String qualifier = instance.qualifier() == null ? "" : instance.qualifier().name();
        qualifier = randomValueOtherThan(qualifier, randomAlphaOfLength(4)::toString);
        return new UnresolvedStar(source, new UnresolvedAttribute(Source.EMPTY, qualifier));
    }

    @Override
    protected UnresolvedStar mutateNameId(UnresolvedStar instance) {
        // Creating a new instance is enough as the NameId is generated automatically.
        return new UnresolvedStar(instance.source(), instance.qualifier());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return true;
    }

    @Override
    protected UnresolvedStar copyInstance(UnresolvedStar instance, TransportVersion version) throws IOException {
        // Doesn't escape the node
        return new UnresolvedStar(instance.source(), instance.qualifier());
    }
}
