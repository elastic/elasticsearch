/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexInfo;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexType;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;
import static org.elasticsearch.xpack.ql.util.StringUtils.SQL_WILDCARD;

public class SysTables extends Command {

    private final String index;
    private final LikePattern pattern;
    private final LikePattern clusterPattern;
    private final EnumSet<IndexType> types;

    public SysTables(Source source, LikePattern clusterPattern, String index, LikePattern pattern, EnumSet<IndexType> types) {
        super(source);
        this.clusterPattern = clusterPattern;
        this.index = index;
        this.pattern = pattern;
        this.types = types;
    }

    @Override
    protected NodeInfo<SysTables> info() {
        return NodeInfo.create(this, SysTables::new, clusterPattern, index, pattern, types);
    }

    @Override
    public List<Attribute> output() {
        return asList(
            keyword("TABLE_CAT"),
            keyword("TABLE_SCHEM"),
            keyword("TABLE_NAME"),
            keyword("TABLE_TYPE"),
            keyword("REMARKS"),
            keyword("TYPE_CAT"),
            keyword("TYPE_SCHEM"),
            keyword("TYPE_NAME"),
            keyword("SELF_REFERENCING_COL_NAME"),
            keyword("REF_GENERATION")
        );
    }

    @Override
    public final void execute(SqlSession session, ActionListener<Page> listener) {
        String cluster = session.indexResolver().clusterName();

        // first check if where dealing with ODBC enumeration
        // namely one param specified with '%', everything else empty string
        // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function?view=ssdt-18vs2017#comments

        // catalog enumeration
        if (clusterPattern == null || clusterPattern.pattern().equals(SQL_WILDCARD)) {
            // enumerate only if pattern is "" and no types are specified (types is null)
            if (pattern != null && pattern.pattern().isEmpty() && index == null && types == null) {
                // include remote and local cluster
                Set<String> clusters = session.indexResolver().remoteClusters();
                clusters.add(cluster);

                List<List<?>> rows = new ArrayList<>(clusters.size());
                for (String name : clusters) {
                    List<String> row = new ArrayList<>(10);
                    row.addAll(Arrays.asList(name, null, null, null, null, null, null, null, null, null));
                    rows.add(row);
                }
                listener.onResponse(of(session, rows));
                return;
            }
        }

        boolean includeFrozen = session.configuration().includeFrozen();

        // enumerate types
        // if no types are specified (the parser takes care of the % case)
        if (types == null) {
            // empty string for catalog
            if (clusterPattern != null && clusterPattern.pattern().isEmpty()
            // empty string for table like and no index specified
                && pattern != null
                && pattern.pattern().isEmpty()
                && index == null) {
                List<List<?>> values = new ArrayList<>();
                // send only the types, everything else is made of empty strings
                // NB: since the types are sent in SQL, frozen doesn't have to be taken into account since
                // it's just another TABLE
                Set<IndexType> typeSet = IndexType.VALID_REGULAR;
                for (IndexType type : typeSet) {
                    Object[] enumeration = new Object[10];
                    enumeration[3] = type.toSql();
                    values.add(asList(enumeration));
                }

                values.sort(Comparator.comparing(l -> l.get(3).toString()));
                listener.onResponse(of(session, values));
                return;
            }
        }

        // no enumeration pattern found, list actual tables
        String cRegex = clusterPattern != null ? clusterPattern.asIndexNameWildcard() : null;
        String idx = hasText(index) ? index : (pattern != null ? pattern.asIndexNameWildcard() : "*");
        String regex = pattern != null ? pattern.asJavaRegex() : null;

        EnumSet<IndexType> tableTypes = types;

        // initialize types for name resolution
        if (tableTypes == null) {
            tableTypes = includeFrozen ? IndexType.VALID_INCLUDE_FROZEN : IndexType.VALID_REGULAR;
        } else {
            if (includeFrozen && tableTypes.contains(IndexType.FROZEN_INDEX) == false) {
                tableTypes.add(IndexType.FROZEN_INDEX);
            }
        }

        session.indexResolver()
            .resolveNames(
                cRegex,
                idx,
                regex,
                tableTypes,
                listener.delegateFailureAndWrap(
                    (delegate, result) -> delegate.onResponse(
                        of(
                            session,
                            result.stream()
                                // sort by type, then by cluster and name
                                .sorted(
                                    Comparator.<IndexInfo, String>comparing(i -> i.type().toSql())
                                        .thenComparing(IndexInfo::cluster)
                                        .thenComparing(IndexInfo::name)
                                )
                                .map(t -> asList(t.cluster(), null, t.name(), t.type().toSql(), EMPTY, null, null, null, null, null))
                                .collect(toList())
                        )
                    )
                )
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterPattern, index, pattern, types);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SysTables other = (SysTables) obj;
        return Objects.equals(clusterPattern, other.clusterPattern)
            && Objects.equals(index, other.index)
            && Objects.equals(pattern, other.pattern)
            && Objects.equals(types, other.types);
    }
}
