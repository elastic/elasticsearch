/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.ArrayList;
import java.util.List;

public class ExpressionCoreWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(expressions());
        entries.addAll(namedExpressions());
        entries.addAll(attributes());
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> expressions() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        // add entries as expressions
        for (NamedWriteableRegistry.Entry e : namedExpressions()) {
            entries.add(new NamedWriteableRegistry.Entry(Expression.class, e.name, in -> (Expression) e.reader.read(in)));
        }
        entries.add(Literal.ENTRY);
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> namedExpressions() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        // add entries as named writeables
        for (NamedWriteableRegistry.Entry e : attributes()) {
            entries.add(new NamedWriteableRegistry.Entry(NamedExpression.class, e.name, in -> (NamedExpression) e.reader.read(in)));
        }
        entries.add(Alias.ENTRY);
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> attributes() {
        return List.of(FieldAttribute.ENTRY, MetadataAttribute.ENTRY, ReferenceAttribute.ENTRY);
    }
}
