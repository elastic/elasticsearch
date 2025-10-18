/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class UnionAll extends Fork implements PostOptimizationPlanVerificationAware {

    private final List<Attribute> output;

    public UnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
        this.output = output;
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new UnionAll(source(), newChildren, output);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, UnionAll::new, children(), output);
    }

    @Override
    public UnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new UnionAll(source(), subPlans, output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(UnionAll.class, children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnionAll other = (UnionAll) o;

        return Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return UnionAll::checkUnionAll;
    }

    private static void checkUnionAll(LogicalPlan plan, Failures failures) {
        if (plan instanceof UnionAll == false) {
            return;
        }
        UnionAll unionAll = (UnionAll) plan;

        Map<String, DataType> outputTypes = unionAll.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));
        Map<String, List<DataType>> childrenTypes = unionAll.children()
            .stream()
            .flatMap(p -> p.output().stream())
            .collect(Collectors.groupingBy(Attribute::name, Collectors.mapping(Attribute::dataType, Collectors.toList())));

        unionAll.children().forEach(subPlan -> {
            for (Attribute attr : subPlan.output()) {
                var expected = outputTypes.get(attr.name());

                // UnionAll with unsupported types should not be allowed here, otherwise runtime couldn't handle it
                // TODO make this a failure
                if (expected == DataType.UNSUPPORTED) {
                    continue;
                    /*
                    failures.add(
                        Failure.fail(
                            attr,
                            "Column [{}] has conflicting data types in subqueries: [{}]",
                            attr.name(),
                            childrenTypes.get(attr.name()).stream().map(Objects::toString).collect(Collectors.joining(", "))
                        )
                    );
                     */
                }

                var actual = attr.dataType();
                if (actual != expected) {
                    failures.add(
                        Failure.fail(
                            attr,
                            "Column [{}] has conflicting data types in subqueries: [{}] and [{}]",
                            attr.name(),
                            actual,
                            expected
                        )
                    );
                }
            }
        });
    }

    private static Map<String, DataType> buildOutputTypes(UnionAll unionAll) {
        return unionAll.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));
    }

    private static Map<String, List<DataType>> buildChildrenTypes(UnionAll unionAll) {
        return unionAll.children()
            .stream()
            .flatMap(p -> p.output().stream())
            .collect(Collectors.groupingBy(Attribute::name, Collectors.mapping(Attribute::dataType, Collectors.toList())));
    }

    private static void checkChildAttributes(
        LogicalPlan child,
        Map<String, DataType> outputTypes,
        Map<String, List<DataType>> childrenTypes,
        Failures failures
    ) {
        for (Attribute attr : child.output()) {
            DataType expected = outputTypes.get(attr.name());

            if (expected == DataType.UNSUPPORTED) {
                reportUnsupportedType(attr, childrenTypes, failures);
                continue;
            }

            DataType actual = attr.dataType();
            if (actual != expected) {
                reportTypeConflict(attr, actual, expected, failures);
            }
        }
    }

    private static void reportUnsupportedType(Attribute attr, Map<String, List<DataType>> childrenTypes, Failures failures) {
        String types = childrenTypes.get(attr.name()).stream().map(Objects::toString).collect(Collectors.joining(", "));

        failures.add(Failure.fail(attr, "Column [{}] has unsupported/conflicting data types in subqueries: [{}]", attr.name(), types));
    }

    private static void reportTypeConflict(Attribute attr, DataType actual, DataType expected, Failures failures) {
        failures.add(
            Failure.fail(attr, "Column [{}] has conflicting data types in subqueries: [{}] vs [{}]", attr.name(), actual, expected)
        );
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return UnionAll::checkNestedUnionAlls;
    }

    private static void checkNestedUnionAlls(LogicalPlan logicalPlan, Failures failures) {
        if (logicalPlan instanceof UnionAll unionAll) {
            unionAll.forEachDown(UnionAll.class, otherUnionAll -> {
                if (unionAll == otherUnionAll) {
                    return;
                }
                failures.add(Failure.fail(otherUnionAll, "Nested subqueries are not supported"));
            });
        }
    }
}
