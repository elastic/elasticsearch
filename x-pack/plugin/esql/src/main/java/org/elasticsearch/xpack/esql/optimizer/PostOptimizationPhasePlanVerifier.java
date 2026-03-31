/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FlattenedEsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FlattenedJsonExtractSupport;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateFlattened;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.IndexMode.LOOKUP;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.dataTypeEquals;

/**
 * Verifies the plan after optimization.
 * This is invoked immediately after a Plan Optimizer completes its work.
 * Currently, it is called after LogicalPlanOptimizer, PhysicalPlanOptimizer,
 * LocalLogicalPlanOptimizer, and LocalPhysicalPlanOptimizer.
 * Note: Logical and Physical optimizers may override methods in this class to perform different checks.
 */
public abstract class PostOptimizationPhasePlanVerifier<P extends QueryPlan<P>> {

    // Are we verifying the global plan (coordinator) or a local plan (data node)?
    protected final boolean isLocal;

    protected PostOptimizationPhasePlanVerifier(boolean isLocal) {
        this.isLocal = isLocal;
    }

    /** Verifies the optimized plan */
    public Failures verify(P optimizedPlan, List<Attribute> expectedOutputAttributes) {
        Failures failures = new Failures();
        Failures depFailures = new Failures();

        checkPlanConsistency(optimizedPlan, failures, depFailures);

        verifyOutputNotChanged(optimizedPlan, expectedOutputAttributes, failures);

        ConfigurationAware.verifyNoMarkerConfiguration(optimizedPlan, failures);

        if (depFailures.hasFailures()) {
            throw new IllegalStateException(depFailures.toString());
        }

        return failures;
    }

    abstract void checkPlanConsistency(P optimizedPlan, Failures failures, Failures depFailures);

    private static void verifyOutputNotChanged(QueryPlan<?> optimizedPlan, List<Attribute> expectedOutputAttributes, Failures failures) {
        // disable this check if there are other failures already
        // it is possible that some of the attributes are not resolved yet and that is reflected in the failures
        // we cannot get the datatype on an unresolved attribute
        // if we try it, it causes an exception and the exception hides the more detailed error message
        if (failures.hasFailures()) {
            return;
        }
        if (dataTypeEquals(expectedOutputAttributes, optimizedPlan.output()) == false) {
            // If the output level is empty we add a column called ProjectAwayColumns.ALL_FIELDS_PROJECTED
            // We will ignore such cases for output verification
            // TODO: this special casing is required due to https://github.com/elastic/elasticsearch/issues/121741, remove when fixed.
            boolean hasProjectAwayColumns = optimizedPlan.output()
                .stream()
                .anyMatch(x -> x.name().equals(ProjectAwayColumns.ALL_FIELDS_PROJECTED));
            // LookupJoinExec represents the lookup index with EsSourceExec and this is turned into EsQueryExec by
            // ReplaceSourceAttributes. Because InsertFieldExtraction doesn't apply to lookup indices, the
            // right hand side will only have the EsQueryExec providing the _doc attribute and nothing else.
            // We perform an optimizer run on every fragment. LookupJoinExec also contains such a fragment,
            // and currently it only contains an EsQueryExec after optimization.
            boolean hasLookupJoinExec = optimizedPlan instanceof EsQueryExec esQueryExec && esQueryExec.indexMode() == LOOKUP;
            // If we group on a text field when using the TS command, we create an Alias that wraps the text field
            // in a Values aggregation. Aggregations will return Keywords as opposed to Text types, so we want to
            // permit the output type changing here.
            boolean hasTextGroupingInTimeSeries = optimizedPlan.anyMatch(
                a -> a instanceof TimeSeriesAggregate ts
                    && ts.aggregates().stream().anyMatch(g -> Alias.unwrap(g) instanceof Values v && v.field().dataType() == DataType.TEXT)
            );
            // TranslateTimeSeriesAggregate may add a _timeseries attribute into the projection.
            boolean hasTimeSeriesReplacingTsId = optimizedPlan.output().stream().anyMatch(MetadataAttribute::isTimeSeriesAttribute)
                && expectedOutputAttributes.stream().noneMatch(MetadataAttribute::isTimeSeriesAttribute);
            // Query approximation can add columns to the output with the confidence intervals.
            boolean hasQueryApproximationAddingColumns = optimizedPlan.anyMatch(plan -> plan instanceof SampledAggregate)
                && dataTypeEquals(expectedOutputAttributes, optimizedPlan.output().subList(0, expectedOutputAttributes.size()));
            // PropagateFlattenedSubfields merges synthetic dotted attributes into EsRelation output for keyed loaders.
            boolean hasPropagatedKeyedFlattenedSubfieldsOnly = differenceIsOnlyPropagatedKeyedFlattenedSubfields(
                expectedOutputAttributes,
                optimizedPlan.output()
            );

            boolean ignoreError = hasProjectAwayColumns
                || hasLookupJoinExec
                || hasTextGroupingInTimeSeries
                || hasTimeSeriesReplacingTsId
                || hasQueryApproximationAddingColumns
                || hasPropagatedKeyedFlattenedSubfieldsOnly;
            if (ignoreError == false) {
                failures.add(
                    fail(
                        optimizedPlan,
                        "Output has changed from [{}] to [{}]. ",
                        expectedOutputAttributes.toString(),
                        optimizedPlan.output().toString()
                    )
                );
            }
        }
    }

    /**
     * True when {@code optimized} is {@code expected} plus only synthetic dotted keyword columns under a
     * {@link FlattenedEsField} root that qualify for keyed flattened loading (same shape as
     * {@link PropagateFlattened}).
     */
    private static boolean differenceIsOnlyPropagatedKeyedFlattenedSubfields(
        List<Attribute> expectedOutputAttributes,
        List<Attribute> optimizedOutput
    ) {
        if (EsqlCapabilities.Cap.JSON_EXTRACT_FLATTENED_FIELD.isEnabled() == false) {
            return false;
        }
        return dataTypeEquals(
            expectedOutputAttributes,
            withoutPropagatedKeyedFlattenedSubfields(expectedOutputAttributes, optimizedOutput)
        );
    }

    private static List<Attribute> withoutPropagatedKeyedFlattenedSubfields(
        List<Attribute> expectedOutputAttributes,
        List<Attribute> optimizedOutput
    ) {
        Set<String> flattenedRoots = new HashSet<>();
        for (Attribute a : expectedOutputAttributes) {
            if (a instanceof FieldAttribute fa && fa.field() instanceof FlattenedEsField) {
                flattenedRoots.add(fa.name());
            }
        }
        if (flattenedRoots.isEmpty()) {
            return optimizedOutput;
        }
        List<Attribute> kept = new ArrayList<>(optimizedOutput.size());
        for (Attribute a : optimizedOutput) {
            if (isPropagatedKeyedFlattenedSubfield(a, flattenedRoots) == false) {
                kept.add(a);
            }
        }
        return kept;
    }

    private static boolean isPropagatedKeyedFlattenedSubfield(Attribute a, Set<String> flattenedRoots) {
        if (a instanceof FieldAttribute fa) {
            if (fa.synthetic() == false) {
                return false;
            }
            if ((fa.field() instanceof KeywordEsField) == false) {
                return false;
            }
            for (String root : flattenedRoots) {
                String prefix = root + ".";
                if (fa.name().startsWith(prefix) && fa.name().length() > prefix.length()) {
                    String subPath = fa.name().substring(prefix.length());
                    return FlattenedJsonExtractSupport.isKeyedFlattenedSubfieldPath(subPath);
                }
            }
        }
        return false;
    }

}
