/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.EsqlConfigurationSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.sameInstance;

public abstract class AbstractAttributeTestCase<T extends Attribute> extends AbstractWireSerializingTestCase<
    AbstractAttributeTestCase.ExtraAttribute> {
    protected abstract T create();

    protected abstract T mutate(T instance);

    @Override
    protected final ExtraAttribute createTestInstance() {
        return new ExtraAttribute(create());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final ExtraAttribute mutateInstance(ExtraAttribute instance) {
        return new ExtraAttribute(mutate((T) instance.a));
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(Attribute.getNamedWriteables());
        entries.add(UnsupportedAttribute.ENTRY);
        entries.addAll(EsField.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected final Writeable.Reader<ExtraAttribute> instanceReader() {
        return ExtraAttribute::new;
    }

    /**
     * Adds extra equality comparisons needed for testing round trips of {@link Attribute}.
     */
    public static class ExtraAttribute implements Writeable {
        private final Attribute a;

        ExtraAttribute(Attribute a) {
            this.a = a;
            assertThat(a.source(), sameInstance(Source.EMPTY));
        }

        ExtraAttribute(StreamInput in) throws IOException {
            PlanStreamInput ps = new PlanStreamInput(
                in,
                PlanNameRegistry.INSTANCE,
                in.namedWriteableRegistry(),
                EsqlConfigurationSerializationTests.randomConfiguration("", Map.of())
            );
            ps.setTransportVersion(in.getTransportVersion());
            a = ps.readNamedWriteable(Attribute.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(a);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return a.equals(null);
            }
            if (obj.getClass() != getClass()) {
                return a.equals(obj);
            }
            ExtraAttribute other = (ExtraAttribute) obj;
            if (false == a.equals(other.a)) {
                return false;
            }
            if (a instanceof FieldAttribute fa && false == fa.field().equals(((FieldAttribute) other.a).field())) {
                return false;
            }
            return a.source() == Source.EMPTY;
        }

        @Override
        public int hashCode() {
            if (a instanceof FieldAttribute fa) {
                return Objects.hash(a, a.source(), fa.field());
            }
            return Objects.hash(a, a.source());
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder(a.toString());
            if (a instanceof FieldAttribute fa) {
                b.append(", field=").append(fa.field());
            }
            return b.toString();
        }
    }
}
