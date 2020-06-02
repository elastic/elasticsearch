/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

public class UnresolvedAttributeTests extends AbstractNodeTestCase<UnresolvedAttribute, Expression> {
    public static UnresolvedAttribute randomUnresolvedAttribute() {
        Source source = SourceTests.randomSource();
        String name = randomAlphaOfLength(5);
        String qualifier = randomQualifier();
        NameId id = randomBoolean() ? null : new NameId();
        String unresolvedMessage = randomUnresolvedMessage();
        Object resolutionMetadata = new Object();
        return new UnresolvedAttribute(source, name, qualifier, id, unresolvedMessage, resolutionMetadata);
    }

    /**
     * A random qualifier. It is important that this be distinct
     * from the name and the unresolvedMessage for testing transform.
     */
    private static String randomQualifier() {
        return randomBoolean() ? null : randomAlphaOfLength(6);
    }

    /**
     * A random qualifier. It is important that this be distinct
     * from the name and the qualifier for testing transform.
     */
    private static String randomUnresolvedMessage() {
        return randomAlphaOfLength(7);
    }

    @Override
    protected UnresolvedAttribute randomInstance() {
        return randomUnresolvedAttribute();
    }

    @Override
    protected UnresolvedAttribute mutate(UnresolvedAttribute a) {
        Supplier<UnresolvedAttribute> option = randomFrom(Arrays.asList(
            () -> new UnresolvedAttribute(a.source(),
                    randomValueOtherThan(a.name(), () -> randomAlphaOfLength(5)),
                    a.qualifier(), a.id(), a.unresolvedMessage(), a.resolutionMetadata()),
            () -> new UnresolvedAttribute(a.source(), a.name(),
                    randomValueOtherThan(a.qualifier(), UnresolvedAttributeTests::randomQualifier),
                    a.id(), a.unresolvedMessage(), a.resolutionMetadata()),
            () -> new UnresolvedAttribute(a.source(), a.name(), a.qualifier(), a.id(),
                    randomValueOtherThan(a.unresolvedMessage(), () -> randomUnresolvedMessage()),
                    a.resolutionMetadata()),
            () -> new UnresolvedAttribute(a.source(), a.name(),
                    a.qualifier(), a.id(), a.unresolvedMessage(), new Object())
        ));
        return option.get();
    }

    @Override
    protected UnresolvedAttribute copy(UnresolvedAttribute a) {
        return new UnresolvedAttribute(a.source(), a.name(), a.qualifier(), a.id(), a.unresolvedMessage(), a.resolutionMetadata());
    }

    @Override
    public void testTransform() {
        UnresolvedAttribute a = randomUnresolvedAttribute();

        String newName = randomValueOtherThan(a.name(), () -> randomAlphaOfLength(5));
        assertEquals(new UnresolvedAttribute(a.source(), newName, a.qualifier(), a.id(),
                a.unresolvedMessage(), a.resolutionMetadata()),
            a.transformPropertiesOnly(v -> Objects.equals(v, a.name()) ? newName : v, Object.class));

        String newQualifier = randomValueOtherThan(a.qualifier(), UnresolvedAttributeTests::randomQualifier);
        assertEquals(new UnresolvedAttribute(a.source(), a.name(), newQualifier, a.id(),
                a.unresolvedMessage(), a.resolutionMetadata()),
            a.transformPropertiesOnly(v -> Objects.equals(v, a.qualifier()) ? newQualifier : v, Object.class));

        NameId newId = new NameId();
        assertEquals(new UnresolvedAttribute(a.source(), a.name(), a.qualifier(), newId,
                a.unresolvedMessage(), a.resolutionMetadata()),
            a.transformPropertiesOnly(v -> Objects.equals(v, a.id()) ? newId : v, Object.class));

        String newMessage = randomValueOtherThan(a.unresolvedMessage(), UnresolvedAttributeTests::randomUnresolvedMessage);
        assertEquals(new UnresolvedAttribute(a.source(), a.name(), a.qualifier(), a.id(),
                newMessage, a.resolutionMetadata()),
            a.transformPropertiesOnly(v -> Objects.equals(v, a.unresolvedMessage()) ? newMessage : v, Object.class));

        Object newMeta = new Object();
        assertEquals(new UnresolvedAttribute(a.source(), a.name(), a.qualifier(), a.id(),
                a.unresolvedMessage(), newMeta),
            a.transformPropertiesOnly(v -> Objects.equals(v, a.resolutionMetadata()) ? newMeta : v, Object.class));
    }

    @Override
    public void testReplaceChildren() {
        // UnresolvedAttribute doesn't have any children
    }
}
