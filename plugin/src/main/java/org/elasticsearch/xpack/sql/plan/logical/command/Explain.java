/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.plan.QueryPlan;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.elasticsearch.xpack.sql.util.Graphviz;

import static java.util.Collections.singletonList;

public class Explain extends Command {

    public enum Type {
        PARSED, ANALYZED, OPTIMIZED, MAPPED, EXECUTABLE, ALL
    }

    public enum Format {
        TEXT, GRAPHVIZ
    }

    private final LogicalPlan plan;
    private final boolean verify;
    private final Format format;
    private final Type type;

    public Explain(Location location, LogicalPlan plan, Type type, Format format, boolean verify) {
        super(location);
        this.plan = plan;
        this.verify = verify;
        this.format = format == null ? Format.TEXT : format;
        this.type = type == null ? Type.ANALYZED : type;
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
        return singletonList(new RootFieldAttribute(location(), "plan", DataTypes.KEYWORD));
    }

    @Override
    protected RowSetCursor execute(SqlSession session) {
        String planString = null;
        String planName = "Parsed";

        if (type == Type.ALL) {
            LogicalPlan analyzedPlan = session.analyzedPlan(plan, verify);
            LogicalPlan optimizedPlan = null;
            PhysicalPlan mappedPlan = null, executionPlan = null;

            Planner planner = session.planner();
            
            // verification is on, exceptions can be thrown
            if (verify) {
                optimizedPlan = session.optimizedPlan(plan);
                mappedPlan = planner.mapPlan(optimizedPlan, verify);
                executionPlan = planner.foldPlan(mappedPlan, verify);
            }
            // check errors manually to see how far the plans work out
            else {
                // no analysis failure, can move on
                if (session.analyzer().verifyFailures(analyzedPlan).isEmpty()) {
                    optimizedPlan = session.optimizedPlan(analyzedPlan);
                    if (optimizedPlan != null) {
                        mappedPlan = planner.mapPlan(optimizedPlan, verify);
                        if (planner.verifyMappingPlanFailures(mappedPlan).isEmpty()) {
                            executionPlan = planner.foldPlan(mappedPlan, verify);
                        }
                    }
                }
            }
            
            if (format == Format.TEXT) {
                StringBuilder sb = new StringBuilder();
                sb.append("Parsed\n");
                sb.append("-----------\n");
                sb.append(plan.toString());
                sb.append("\nAnalyzed\n");
                sb.append("--------\n");
                sb.append(analyzedPlan.toString());
                sb.append("\nOptimized\n");
                sb.append("---------\n");
                sb.append(optimizedPlan.toString());
                sb.append("\nMapped\n");
                sb.append("---------\n");
                sb.append(mappedPlan.toString());
                sb.append("\nExecutable\n");
                sb.append("---------\n");
                sb.append(executionPlan.toString());
                
                planString = sb.toString();
            }
            else {
                Map<String, QueryPlan<?>> plans = CollectionUtils.of("Parsed", plan, "Analyzed", analyzedPlan, "Optimized", optimizedPlan, "Mapped", mappedPlan, "Execution", executionPlan);
                planString = Graphviz.dot(plans, false);
            }
        }

        else {
            QueryPlan<?> queryPlan = null;

            switch (type) {
                case PARSED:
                    queryPlan = plan;
                    planName = "Parsed";
                    break;
                case ANALYZED:
                    queryPlan = session.analyzedPlan(plan, verify);
                    planName = "Analyzed";
                    break;
                case OPTIMIZED:
                    queryPlan = session.optimizedPlan(session.analyzedPlan(plan, verify));
                    planName = "Optimized";
                    break;
                case MAPPED:
                    queryPlan = session.planner().mapPlan(session.optimizedPlan(session.analyzedPlan(plan, verify)), verify);
                    planName = "Mapped";
                    break;
                case EXECUTABLE:
                    queryPlan = session.planner().foldPlan(session.planner().mapPlan(session.optimizedPlan(session.analyzedPlan(plan, verify)), verify), verify);
                    planName = "Executable";
                    break;

                default:
                    break;
            }

            planString = (format == Format.TEXT ? queryPlan.toString() : Graphviz.dot(planName, queryPlan));
        }

        return Rows.singleton(output(), planString);
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