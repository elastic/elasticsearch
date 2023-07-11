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
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor.Batch;
import org.elasticsearch.xpack.ql.rule.RuleExecutor.ExecutionInfo;
import org.elasticsearch.xpack.ql.rule.RuleExecutor.Transformation;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.util.Graphviz;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class Debug extends Command {

    public enum Type {
        ANALYZED,
        OPTIMIZED;
    }

    public enum Format {
        TEXT,
        GRAPHVIZ
    }

    private final LogicalPlan plan;
    private final Format format;
    private final Type type;

    public Debug(Source source, LogicalPlan plan, Type type, Format format) {
        super(source);
        this.plan = plan;
        this.format = format == null ? Format.TEXT : format;
        this.type = type == null ? Type.OPTIMIZED : type;
    }

    @Override
    protected NodeInfo<Debug> info() {
        return NodeInfo.create(this, Debug::new, plan, type, format);
    }

    public LogicalPlan plan() {
        return plan;
    }

    public Format format() {
        return format;
    }

    public Type type() {
        return type;
    }

    @Override
    public List<Attribute> output() {
        return singletonList(new FieldAttribute(source(), "plan", new KeywordEsField("plan")));
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        switch (type) {
            case ANALYZED -> session.debugAnalyzedPlan(plan, listener.delegateFailureAndWrap((l, i) -> handleInfo(i, l)));
            case OPTIMIZED -> session.analyzedPlan(
                plan,
                true,
                listener.delegateFailureAndWrap((l, analyzedPlan) -> handleInfo(session.optimizer().debugOptimize(analyzedPlan), l))
            );
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void handleInfo(ExecutionInfo info, ActionListener<Page> listener) {
        String planString = null;

        if (format == Format.TEXT) {
            StringBuilder sb = new StringBuilder();
            if (info == null) {
                sb.append(plan.toString());
            } else {
                Map<Batch, List<Transformation>> map = info.transformations();

                for (Entry<Batch, List<Transformation>> entry : map.entrySet()) {
                    // for each batch
                    sb.append("***");
                    sb.append(entry.getKey().name());
                    sb.append("***");
                    for (Transformation tf : entry.getValue()) {
                        sb.append(tf.name());
                        sb.append("\n");
                        sb.append(NodeUtils.diffString(tf.before(), tf.after()));
                        sb.append("\n");
                    }
                }
            }
            planString = sb.toString();
        } else {
            if (info == null) {
                planString = Graphviz.dot("Planned", plan);
            } else {
                Map<String, Node<?>> plans = new LinkedHashMap<>();
                Map<Batch, List<Transformation>> map = info.transformations();
                plans.put("start", info.before());

                for (Entry<Batch, List<Transformation>> entry : map.entrySet()) {
                    // for each batch
                    int counter = 0;
                    for (Transformation tf : entry.getValue()) {
                        if (tf.hasChanged()) {
                            plans.put(tf.name() + "#" + ++counter, tf.after());
                        }
                    }
                }
                planString = Graphviz.dot(plans, true);
            }
        }

        listener.onResponse(Page.last(Rows.singleton(output(), planString)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan, type, format);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Debug o = (Debug) obj;
        return Objects.equals(format, o.format) && Objects.equals(type, o.type) && Objects.equals(plan, o.plan);
    }
}
