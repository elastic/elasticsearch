/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexType;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class ShowTables extends Command {

    private final String index;
    private final LikePattern pattern;
    private final boolean includeFrozen;

    public ShowTables(Source source, String index, LikePattern pattern, boolean includeFrozen) {
        super(source);
        this.index = index;
        this.pattern = pattern;
        this.includeFrozen = includeFrozen;
    }

    @Override
    protected NodeInfo<ShowTables> info() {
        return NodeInfo.create(this, ShowTables::new, index, pattern, includeFrozen);
    }

    public String index() {
        return index;
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(keyword("name"), keyword("type"), keyword("kind"));
    }

    @Override
    public final void execute(SqlSession session, ActionListener<Page> listener) {
        String idx = index != null ? index : (pattern != null ? pattern.asIndexNameWildcard() : "*");
        String regex = pattern != null ? pattern.asJavaRegex() : null;

        // to avoid redundancy, indicate whether frozen fields are required by specifying the type
        EnumSet<IndexType> withFrozen = session.configuration().includeFrozen() || includeFrozen ?
                IndexType.VALID_INCLUDE_FROZEN : IndexType.VALID_REGULAR;
        
        session.indexResolver().resolveNames(idx, regex, withFrozen, ActionListener.wrap(result -> {
            listener.onResponse(of(session, result.stream()
                 .map(t -> asList(t.name(), t.type().toSql(), t.type().toNative()))
                .collect(toList())));
        }, listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, pattern, includeFrozen);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ShowTables other = (ShowTables) obj;
        return Objects.equals(index, other.index)
                && Objects.equals(pattern, other.pattern)
                && includeFrozen == other.includeFrozen;
    }
}