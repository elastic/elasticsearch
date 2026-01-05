/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Represents all entries in a row source command
 */
public class Rows extends LogicalPlan implements TelemetryAware {

    /**
     * represents an entry in a ROW source command
     */
    public static final class Row extends LeafPlan implements PostAnalysisVerificationAware {
        private final List<Alias> fields;

        public Row(Source source, List<Alias> fields) {
            super(source);
            this.fields = fields;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("not serialized");
        }

        public List<Alias> fields() {
            return fields;
        }

        @Override
        public List<Attribute> output() {
            return Expressions.asAttributes(fields);
        }

        @Override
        public boolean expressionsResolved() {
            return Resolvables.resolved(fields);
        }

        @Override
        protected NodeInfo<? extends LogicalPlan> info() {
            return NodeInfo.create(this, Row::new, fields);
        }

        @Override
        public void postAnalysisVerification(Failures failures) {
            for (Alias alias : fields) {
                if (DataType.isRepresentable(alias.dataType()) == false) {
                    failures.add(fail(alias.child(), "cannot use [{}] directly in a row assignment", alias.child().sourceText()));
                }
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (Row) obj;
            return Objects.equals(this.fields, that.fields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fields);
        }
    }

    private final List<Row> entries;

    public Rows(Source source, List<Row> entries) {
        super(source, new ArrayList<>(entries));
        Check.notNull(entries, "Row source command should always have entries");
        this.entries = entries;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<Row> entries() {
        return entries;
    }

    public Row getFirst() {
        return entries.getFirst();
    }

    @Override
    public List<Attribute> output() {
        Map<String, Attribute> attributeMap = new LinkedHashMap<>();
        for (var row : entries) {
            for (var attribute : row.output()) {
                attributeMap.putIfAbsent(attribute.name(), attribute);
            }
        }
        return new ArrayList<>(attributeMap.values());
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(entries);
    }

    @Override
    public final LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        List<Row> rows = new ArrayList<>();
        for (LogicalPlan child : newChildren) {
            if (child instanceof Row) {
                rows.add((Row) child);
            }
        }
        return new Rows(source(), rows);
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Rows::new, entries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rows constant = (Rows) o;
        return Objects.equals(entries, constant.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    public static Rows singleRow(Source source, List<Alias> fields) {
        return new Rows(source, List.of(new Row(source, fields)));
    }
}
