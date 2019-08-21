/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.KeywordEsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class ShowColumns extends Command {

    private final String index;
    private final LikePattern pattern;
    private final boolean includeFrozen;

    public ShowColumns(Source source, String index, LikePattern pattern, boolean includeFrozen) {
        super(source);
        this.index = index;
        this.pattern = pattern;
        this.includeFrozen = includeFrozen;
    }

    public String index() {
        return index;
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    protected NodeInfo<ShowColumns> info() {
        return NodeInfo.create(this, ShowColumns::new, index, pattern, includeFrozen);
    }

    @Override
    public List<Attribute> output() {
        return asList(new FieldAttribute(source(), "column", new KeywordEsField("column")),
                new FieldAttribute(source(), "type", new KeywordEsField("type")),
                new FieldAttribute(source(), "mapping", new KeywordEsField("mapping")));
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        String idx = index != null ? index : (pattern != null ? pattern.asIndexNameWildcard() : "*");
        String regex = pattern != null ? pattern.asJavaRegex() : null;

        boolean withFrozen = includeFrozen || session.configuration().includeFrozen();
        session.indexResolver().resolveAsMergedMapping(idx, regex, withFrozen, ActionListener.wrap(
                indexResult -> {
                    List<List<?>> rows = emptyList();
                    if (indexResult.isValid()) {
                        rows = new ArrayList<>();
                        fillInRows(indexResult.get().mapping(), null, rows);
                    }
                    listener.onResponse(of(session, rows));
                },
                listener::onFailure));
    }

    private void fillInRows(Map<String, EsField> mapping, String prefix, List<List<?>> rows) {
        for (Entry<String, EsField> e : mapping.entrySet()) {
            EsField field = e.getValue();
            DataType dt = field.getDataType();
            String name = e.getKey();
            if (dt != null) {
                // show only fields that exist in ES
                rows.add(asList(prefix != null ? prefix + "." + name : name, dt.sqlName(), dt.typeName));
                if (field.getProperties().isEmpty() == false) {
                    String newPrefix = prefix != null ? prefix + "." + name : name;
                    fillInRows(field.getProperties(), newPrefix, rows);
                }
            }
        }
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

        ShowColumns other = (ShowColumns) obj;
        return Objects.equals(index, other.index)
                && Objects.equals(pattern, other.pattern)
                && includeFrozen == other.includeFrozen;
    }
}
