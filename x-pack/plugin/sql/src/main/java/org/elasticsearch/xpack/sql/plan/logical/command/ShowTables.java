/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import static org.elasticsearch.common.Strings.hasText;

public class ShowTables extends Command {

    private final LikePattern catalogPattern;
    private final String catalog;
    private final String index;
    private final LikePattern pattern;
    private final boolean includeFrozen;

    public ShowTables(Source source, LikePattern catalogPattern, String catalog, String index, LikePattern pattern, boolean includeFrozen) {
        super(source);
        this.catalogPattern = catalogPattern;
        this.catalog = catalog;
        this.index = index;
        this.pattern = pattern;
        this.includeFrozen = includeFrozen;
    }

    @Override
    protected NodeInfo<ShowTables> info() {
        return NodeInfo.create(this, ShowTables::new, catalogPattern, catalog, index, pattern, includeFrozen);
    }

    public String index() {
        return index;
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(keyword("catalog"), keyword("name"), keyword("type"), keyword("kind"));
    }

    @Override
    public final void execute(SqlSession session, ActionListener<Page> listener) {
        String cat = catalogPattern != null ? catalogPattern.asIndexNameWildcard() : catalog;
        cat = hasText(cat) ? cat : session.configuration().catalog();
        String idx = index != null ? index : (pattern != null ? pattern.asIndexNameWildcard() : "*");
        String regex = pattern != null ? pattern.asJavaRegex() : null;

        boolean withFrozen = session.configuration().includeFrozen() || includeFrozen;
        // to avoid redundancy, indicate whether frozen fields are required by specifying the type
        EnumSet<IndexType> indexType = withFrozen ? IndexType.VALID_INCLUDE_FROZEN : IndexType.VALID_REGULAR;
        session.indexResolver().resolveNames(cat, idx, regex, indexType, listener.delegateFailureAndWrap((delegate, result) -> {
            delegate.onResponse(
                of(
                    session,
                    result.stream().map(t -> asList(t.cluster(), t.name(), t.type().toSql(), t.type().toNative())).collect(toList())
                )
            );
        }));
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
        return Objects.equals(index, other.index) && Objects.equals(pattern, other.pattern) && includeFrozen == other.includeFrozen;
    }
}
