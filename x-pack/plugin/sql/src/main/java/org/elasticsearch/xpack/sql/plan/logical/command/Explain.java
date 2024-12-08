/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.plan.QueryPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.util.Graphviz;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;

public class Explain extends Command {

    public enum Type {
        PARSED,
        ANALYZED,
        OPTIMIZED,
        MAPPED,
        EXECUTABLE,
        ALL;

        public String printableName() {
            return Strings.capitalize(name().toLowerCase(Locale.ROOT));
        }
    }

    public enum Format {
        TEXT,
        GRAPHVIZ
    }

    private final LogicalPlan plan;
    private final boolean verify;
    private final Format format;
    private final Type type;

    public Explain(Source source, LogicalPlan plan, Type type, Format format, boolean verify) {
        super(source);
        this.plan = plan;
        this.verify = verify;
        this.format = format == null ? Format.TEXT : format;
        this.type = type == null ? Type.ANALYZED : type;
    }

    @Override
    protected NodeInfo<Explain> info() {
        return NodeInfo.create(this, Explain::new, plan, type, format, verify);
    }

    public LogicalPlan plan() {
        return plan;
    }

    public boolean verify() {
        return verify;
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

        if (type == Type.PARSED) {
            listener.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, plan))));
            return;
        }

        // to avoid duplicating code, the type/verification filtering happens inside the listeners instead of outside using a CASE
        session.analyzedPlan(plan, verify, listener.delegateFailureAndWrap((delegate, analyzedPlan) -> {

            if (type == Type.ANALYZED) {
                delegate.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, analyzedPlan))));
                return;
            }

            Planner planner = session.planner();
            // verification is on, exceptions can be thrown
            if (verify) {
                session.optimizedPlan(analyzedPlan, delegate.delegateFailureAndWrap((delegate2, optimizedPlan) -> {
                    if (type == Type.OPTIMIZED) {
                        delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, optimizedPlan))));
                        return;
                    }

                    PhysicalPlan mappedPlan = planner.mapPlan(optimizedPlan, verify);
                    if (type == Type.MAPPED) {
                        delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, mappedPlan))));
                        return;
                    }

                    PhysicalPlan executablePlan = planner.foldPlan(mappedPlan, verify);
                    if (type == Type.EXECUTABLE) {
                        delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, executablePlan))));
                        return;
                    }

                    // Type.All
                    delegate2.onResponse(
                        Page.last(
                            Rows.singleton(output(), printPlans(format, plan, analyzedPlan, optimizedPlan, mappedPlan, executablePlan))
                        )
                    );
                }));
            }

            // check errors manually to see how far the plans work out
            else {
                // no analysis failure, can move on
                if (session.verifier().verifyFailures(analyzedPlan, session.configuration().version()).isEmpty()) {
                    session.optimizedPlan(analyzedPlan, delegate.delegateFailureAndWrap((delegate2, optimizedPlan) -> {
                        if (type == Type.OPTIMIZED) {
                            delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, optimizedPlan))));
                            return;
                        }

                        PhysicalPlan mappedPlan = planner.mapPlan(optimizedPlan, verify);

                        if (type == Type.MAPPED) {
                            delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, mappedPlan))));
                            return;
                        }

                        if (planner.verifyMappingPlanFailures(mappedPlan).isEmpty()) {
                            PhysicalPlan executablePlan = planner.foldPlan(mappedPlan, verify);

                            if (type == Type.EXECUTABLE) {
                                delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, executablePlan))));
                                return;
                            }

                            delegate2.onResponse(
                                Page.last(
                                    Rows.singleton(
                                        output(),
                                        printPlans(format, plan, analyzedPlan, optimizedPlan, mappedPlan, executablePlan)
                                    )
                                )
                            );
                            return;
                        }
                        // mapped failed
                        if (type != Type.ALL) {
                            delegate2.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, mappedPlan))));
                            return;
                        }

                        delegate2.onResponse(
                            Page.last(Rows.singleton(output(), printPlans(format, plan, analyzedPlan, optimizedPlan, mappedPlan, null)))
                        );
                    }));
                    // cannot continue
                } else {
                    if (type != Type.ALL) {
                        delegate.onResponse(Page.last(Rows.singleton(output(), formatPlan(format, analyzedPlan))));
                    } else {
                        delegate.onResponse(Page.last(Rows.singleton(output(), printPlans(format, plan, analyzedPlan, null, null, null))));
                    }
                }
            }
        }));
    }

    private static String printPlans(
        Format format,
        LogicalPlan parsed,
        LogicalPlan analyzedPlan,
        LogicalPlan optimizedPlan,
        PhysicalPlan mappedPlan,
        PhysicalPlan executionPlan
    ) {
        if (format == Format.TEXT) {
            StringBuilder sb = new StringBuilder();
            sb.append("Parsed\n");
            sb.append("-----------\n");
            sb.append(parsed.toString());
            sb.append("\nAnalyzed\n");
            sb.append("--------\n");
            sb.append(analyzedPlan.toString());
            sb.append("\nOptimized\n");
            sb.append("---------\n");
            sb.append(nullablePlan(optimizedPlan));
            sb.append("\nMapped\n");
            sb.append("---------\n");
            sb.append(nullablePlan(mappedPlan));
            sb.append("\nExecutable\n");
            sb.append("---------\n");
            sb.append(nullablePlan(executionPlan));

            return sb.toString();
        } else {
            Map<String, QueryPlan<?>> plans = new HashMap<>();
            plans.put("Parsed", parsed);
            plans.put("Analyzed", analyzedPlan);

            if (optimizedPlan != null) {
                plans.put("Optimized", optimizedPlan);
                plans.put("Mapped", mappedPlan);
                plans.put("Execution", executionPlan);
            }
            return Graphviz.dot(unmodifiableMap(plans), false);
        }
    }

    private static String nullablePlan(QueryPlan<?> plan) {
        return plan != null ? plan.toString() : "<not computed due to failures>";
    }

    private String formatPlan(Format format, QueryPlan<?> plan) {
        return (format == Format.TEXT ? nullablePlan(plan) : Graphviz.dot(type.printableName(), plan));
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan, type, format, verify);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Explain o = (Explain) obj;
        return Objects.equals(verify, o.verify)
            && Objects.equals(format, o.format)
            && Objects.equals(type, o.type)
            && Objects.equals(plan, o.plan);
    }
}
