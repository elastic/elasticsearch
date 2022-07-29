/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.ListCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlExecutable;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class Command extends LogicalPlan implements SqlExecutable {

    protected Command(Source source) {
        super(source, emptyList());
    }

    @Override
    public final LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    /**
     * Syntactic sugar for creating a schema keyword/string field.
     */
    protected final FieldAttribute keyword(String name) {
        return field(name, new KeywordEsField(name));
    }

    /**
     * Syntactic sugar for creating a schema field.
     */
    protected final FieldAttribute field(String name, DataType type) {
        return field(name, new EsField(name, type, emptyMap(), true));
    }

    private FieldAttribute field(String name, EsField field) {
        return new FieldAttribute(source(), name, field);
    }

    protected Page of(SqlSession session, List<List<?>> values) {
        return ListCursor.of(Rows.schema(output()), values, session.configuration().pageSize());
    }
}
