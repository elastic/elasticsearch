/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class UnresolvedNamePatternTests extends AbstractNamedExpressionSerializationTests<UnresolvedNamePattern> {
    @Override
    protected UnresolvedNamePattern createTestInstance() {
        // No automaton, this is normally injected during parsing and is derived from the pattern.
        return new UnresolvedNamePattern(Source.EMPTY, null, randomAlphaOfLength(3), randomAlphaOfLength(3));
    }

    @Override
    protected UnresolvedNamePattern mutateInstance(UnresolvedNamePattern instance) {
        Source source = instance.source();
        String name = instance.name();
        String pattern = instance.pattern();
        switch (between(0, 1)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(4));
            case 1 -> pattern = randomValueOtherThan(pattern, () -> randomAlphaOfLength(4));
        }
        return new UnresolvedNamePattern(source, null, name, pattern);
    }

    @Override
    protected UnresolvedNamePattern mutateNameId(UnresolvedNamePattern instance) {
        // Creating a new instance is enough as the NameId is generated automatically.
        return new UnresolvedNamePattern(instance.source(), null, instance.pattern(), instance.name());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return true;
    }

    @Override
    protected UnresolvedNamePattern copyInstance(UnresolvedNamePattern instance, TransportVersion version) throws IOException {
        // Doesn't escape the node
        return new UnresolvedNamePattern(instance.source(), null, instance.pattern(), instance.name());
    }
}
