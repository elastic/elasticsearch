/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;

import java.io.IOException;
import java.util.Objects;

public class UnresolvedAttributeTests extends AbstractNamedExpressionSerializationTests<UnresolvedAttribute> {
    public static UnresolvedAttribute randomUnresolvedAttribute() {
        Source source = SourceTests.randomSource();
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(5);
        String name = randomAlphaOfLength(5);
        NameId id = randomBoolean() ? null : new NameId();
        String unresolvedMessage = randomUnresolvedMessage();
        return new UnresolvedAttribute(source, qualifier, name, id, unresolvedMessage);
    }

    /**
     * A random qualifier. It is important that this be distinct
     * from the name and the qualifier for testing transform.
     */
    private static String randomUnresolvedMessage() {
        return randomAlphaOfLength(7);
    }

    @Override
    protected UnresolvedAttribute createTestInstance() {
        return randomUnresolvedAttribute();
    }

    @Override
    protected UnresolvedAttribute mutateInstance(UnresolvedAttribute instance) {
        Source source = instance.source();
        String name = instance.name();
        String qualifier = instance.qualifier();
        NameId id = instance.id();
        String unresolvedMessage = instance.unresolvedMessage();

        switch (between(0, 3)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomBoolean() ? null : randomAlphaOfLength(5));
            case 1 -> qualifier = randomAlphaOfLength(qualifier == null ? 3 : qualifier.length() + 1);
            case 2 -> id = new NameId();
            case 3 -> unresolvedMessage = randomValueOtherThan(unresolvedMessage, UnresolvedAttributeTests::randomUnresolvedMessage);
        }
        return new UnresolvedAttribute(source, qualifier, name, id, unresolvedMessage);
    }

    @Override
    protected UnresolvedAttribute copyInstance(UnresolvedAttribute instance, TransportVersion version) throws IOException {
        // Doesn't escape the node
        return new UnresolvedAttribute(
            instance.source(),
            instance.qualifier(),
            instance.name(),
            instance.id(),
            instance.unresolvedMessage()
        );
    }

    @Override
    protected UnresolvedAttribute mutateNameId(UnresolvedAttribute instance) {
        return instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }

    public void testTransform() {
        UnresolvedAttribute a = randomUnresolvedAttribute();

        String newName = randomValueOtherThan(a.name(), () -> randomAlphaOfLength(5));
        assertEquals(
            new UnresolvedAttribute(a.source(), a.qualifier(), newName, a.id(), a.unresolvedMessage()),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.name()) ? newName : v)
        );

        NameId newId = new NameId();
        assertEquals(
            new UnresolvedAttribute(a.source(), a.qualifier(), a.name(), newId, a.unresolvedMessage()),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.id()) ? newId : v)
        );

        String newMessage = randomValueOtherThan(a.unresolvedMessage(), UnresolvedAttributeTests::randomUnresolvedMessage);
        assertEquals(
            new UnresolvedAttribute(a.source(), a.qualifier(), a.name(), a.id(), newMessage),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.unresolvedMessage()) ? newMessage : v)
        );
    }
}
