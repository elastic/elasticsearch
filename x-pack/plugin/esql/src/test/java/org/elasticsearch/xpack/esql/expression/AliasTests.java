/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class AliasTests extends AbstractWireTestCase<Alias> {
    public static Alias randomAlias() {
        Source source = SourceTests.randomSource();
        String name = randomAlphaOfLength(5);
        // TODO better randomChild
        Expression child = ReferenceAttributeTests.randomReferenceAttribute();
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
            case 1 -> child = randomValueOtherThan(child, ReferenceAttributeTests::randomReferenceAttribute);
            case 2 -> synthetic = false == synthetic;
        }
        return new Alias(source, name, child, instance.id(), synthetic);
    }

    @Override
    protected Alias copyInstance(Alias instance, TransportVersion version) throws IOException {
        return copyInstance(
            instance,
            getNamedWriteableRegistry(),
            (out, v) -> new PlanStreamOutput(out, new PlanNameRegistry(), null).writeNamedWriteable(v),
            in -> {
                PlanStreamInput pin = new PlanStreamInput(in, new PlanNameRegistry(), in.namedWriteableRegistry(), null);
                Alias deser = (Alias) pin.readNamedWriteable(NamedExpression.class);
                assertThat(deser.id(), equalTo(pin.mapNameId(Long.parseLong(instance.id().toString()))));
                return deser;
            },
            version
        );
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(NamedExpression.getNamedWriteables());
        entries.addAll(Attribute.getNamedWriteables());
        entries.add(UnsupportedAttribute.ENTRY);
        entries.addAll(Expression.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }
}
