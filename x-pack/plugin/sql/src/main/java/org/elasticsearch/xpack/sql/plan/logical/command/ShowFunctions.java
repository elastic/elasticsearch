/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionTypeRegistry;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class ShowFunctions extends Command {

    private final LikePattern pattern;

    public ShowFunctions(Source source, LikePattern pattern) {
        super(source);
        this.pattern = pattern;
    }

    @Override
    protected NodeInfo<ShowFunctions> info() {
        return NodeInfo.create(this, ShowFunctions::new, pattern);
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(new FieldAttribute(source(), "name", new KeywordEsField("name")),
                new FieldAttribute(source(), "type", new KeywordEsField("type")));
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        FunctionRegistry registry = session.functionRegistry();
        Collection<FunctionDefinition> functions = registry.listFunctions(pattern != null ? pattern.asJavaRegex() : null);
        listener.onResponse(of(session, functions.stream()
                .map(f -> asList(f.name(), SqlFunctionTypeRegistry.INSTANCE.type(f.clazz())))
                .collect(toList())));
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ShowFunctions other = (ShowFunctions) obj;
        return Objects.equals(pattern, other.pattern);
    }
}
