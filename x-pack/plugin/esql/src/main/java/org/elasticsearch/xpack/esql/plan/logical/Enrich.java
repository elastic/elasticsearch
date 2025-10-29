/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Enrich extends UnaryPlan
    implements
        GeneratingPlan<Enrich>,
        PostOptimizationVerificationAware.CoordinatorOnly,
        PostAnalysisVerificationAware,
        TelemetryAware,
        Streaming,
        SortAgnostic,
        ExecutesOn {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Enrich",
        Enrich::readFrom
    );
    // policyName can only be a string literal once it's resolved
    private final Expression policyName;
    private final NamedExpression matchField;
    private final EnrichPolicy policy;
    private final Map<String, String> concreteIndices; // cluster -> enrich indices
    // This could be simplified by just always using an Alias.
    private final List<NamedExpression> enrichFields;
    private List<Attribute> output;

    private final Mode mode;

    @Override
    public ExecuteLocation executesOn() {
        return switch (mode) {
            case REMOTE -> ExecuteLocation.REMOTE;
            case COORDINATOR -> ExecuteLocation.COORDINATOR;
            default -> ExecuteLocation.ANY;
        };
    }

    public enum Mode {
        ANY,
        COORDINATOR,
        REMOTE;

        private static final Map<String, Mode> map;

        static {
            var values = Mode.values();
            map = Maps.newMapWithExpectedSize(values.length);
            for (Mode m : values) {
                map.put(m.name(), m);
            }
        }

        public static Mode from(String name) {
            return name == null ? null : map.get(name.toUpperCase(Locale.ROOT));
        }
    }

    public Enrich(
        Source source,
        LogicalPlan child,
        Mode mode,
        Expression policyName,
        NamedExpression matchField,
        EnrichPolicy policy,
        Map<String, String> concreteIndices,
        List<NamedExpression> enrichFields
    ) {
        super(source, child);
        this.mode = mode == null ? Mode.ANY : mode;
        this.policyName = policyName;
        this.matchField = matchField;
        this.policy = policy;
        this.concreteIndices = concreteIndices;
        this.enrichFields = enrichFields;
    }

    private static Enrich readFrom(StreamInput in) throws IOException {
        Enrich.Mode mode = in.readEnum(Enrich.Mode.class);
        final Source source = Source.readFrom((PlanStreamInput) in);
        final LogicalPlan child = in.readNamedWriteable(LogicalPlan.class);
        final Expression policyName = in.readNamedWriteable(Expression.class);
        final NamedExpression matchField = in.readNamedWriteable(NamedExpression.class);
        final EnrichPolicy policy = new EnrichPolicy(in);
        final Map<String, String> concreteIndices = in.readMap(StreamInput::readString, StreamInput::readString);
        return new Enrich(
            source,
            child,
            mode,
            policyName,
            matchField,
            policy,
            concreteIndices,
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(mode());
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(policyName());
        out.writeNamedWriteable(matchField());
        policy().writeTo(out);
        out.writeMap(concreteIndices(), StreamOutput::writeString, StreamOutput::writeString);
        out.writeNamedWriteableCollection(enrichFields());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public NamedExpression matchField() {
        return matchField;
    }

    public List<NamedExpression> enrichFields() {
        return enrichFields;
    }

    public EnrichPolicy policy() {
        return policy;
    }

    public Map<String, String> concreteIndices() {
        return concreteIndices;
    }

    public Expression policyName() {
        return policyName;
    }

    public String resolvedPolicyName() {
        return BytesRefs.toString(literalValueOf(policyName));
    }

    public Mode mode() {
        return mode;
    }

    @Override
    protected AttributeSet computeReferences() {
        return matchField.references();
    }

    @Override
    public boolean expressionsResolved() {
        return policyName.resolved()
            && matchField instanceof EmptyAttribute == false // matchField not defined in the query, needs to be resolved from the policy
            && matchField.resolved()
            && Resolvables.resolved(enrichFields());
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Enrich(source(), newChild, mode, policyName, matchField, policy, concreteIndices, enrichFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Enrich::new, child(), mode, policyName, matchField, policy, concreteIndices, enrichFields);
    }

    @Override
    public List<Attribute> output() {
        if (enrichFields == null) {
            return child().output();
        }
        if (this.output == null) {
            this.output = mergeOutputAttributes(enrichFields(), child().output());
        }
        return output;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return asAttributes(enrichFields);
    }

    @Override
    public Enrich withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);

        List<NamedExpression> newEnrichFields = new ArrayList<>(enrichFields.size());
        for (int i = 0; i < enrichFields.size(); i++) {
            NamedExpression enrichField = enrichFields.get(i);
            String newName = newNames.get(i);
            if (enrichField.name().equals(newName)) {
                newEnrichFields.add(enrichField);
            } else if (enrichField instanceof ReferenceAttribute ra) {
                newEnrichFields.add(new Alias(ra.source(), newName, ra, new NameId(), ra.synthetic()));
            } else if (enrichField instanceof Alias a) {
                newEnrichFields.add(new Alias(a.source(), newName, a.child(), new NameId(), a.synthetic()));
            } else {
                throw new IllegalArgumentException("Enrich field must be Alias or ReferenceAttribute");
            }
        }
        return new Enrich(source(), child(), mode(), policyName(), matchField(), policy(), concreteIndices(), newEnrichFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Enrich enrich = (Enrich) o;
        return Objects.equals(mode, enrich.mode)
            && Objects.equals(policyName, enrich.policyName)
            && Objects.equals(matchField, enrich.matchField)
            && Objects.equals(policy, enrich.policy)
            && Objects.equals(concreteIndices, enrich.concreteIndices)
            && Objects.equals(enrichFields, enrich.enrichFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mode, policyName, matchField, policy, concreteIndices, enrichFields);
    }

    /**
     * Ensure that no remote enrich is allowed after a reduction or an enrich with coordinator mode.
     * <p>
     * TODO:
     * For Limit and TopN, we can insert the same node after the remote enrich (also needs to move projections around)
     * to eliminate this limitation. Otherwise, we force users to write queries that might not perform well.
     * For example, `FROM test | ORDER @timestamp | LIMIT 10 | ENRICH _remote:` doesn't work.
     * In that case, users have to write it as `FROM test | ENRICH _remote: | ORDER @timestamp | LIMIT 10`,
     * which is equivalent to bringing all data to the coordinating cluster.
     * We might consider implementing the actual remote enrich on the coordinating cluster, however, this requires
     * retaining the originating cluster and restructuring pages for routing, which might be complicated.
     */
    private void checkForPlansForbiddenBeforeRemoteEnrich(Failures failures) {
        Set<Source> fails = new HashSet<>();

        this.forEachDown(LogicalPlan.class, u -> {
            if (u instanceof ExecutesOn ex && ex.executesOn() == ExecuteLocation.COORDINATOR) {
                failures.add(
                    fail(this, "ENRICH with remote policy can't be executed after [" + u.source().text() + "]" + u.source().source())
                );
            }
        });
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (this.mode == Mode.REMOTE) {
            checkMvExpandAfterLimit(failures);
        }

    }

    /**
     * Remote ENRICH (and any remote operation in fact) is not compatible with MV_EXPAND + LIMIT. Consider:
     * `FROM *:events | SORT @timestamp | LIMIT 2 | MV_EXPAND ip | ENRICH _remote:clientip_policy ON ip`
     * Semantically, this must take two top events and then expand them. However, this can not be executed remotely,
     * because this means that we have to take top 2 events on each node, then expand them, then apply Enrich,
     * then bring them to the coordinator - but then we can not select top 2 of them - because that would be pre-expand!
     * We do not know which expanded rows are coming from the true top rows and which are coming from "false" top rows
     * which should have been thrown out. This is only possible to execute if MV_EXPAND executes on the coordinator
     * - which contradicts remote Enrich.
     * This could be fixed by the optimizer by moving MV_EXPAND past ENRICH, at least in some cases, but currently we do not do that.
     */
    private void checkMvExpandAfterLimit(Failures failures) {
        this.forEachDown(MvExpand.class, u -> {
            u.forEachDown(p -> {
                if (p instanceof Limit || p instanceof TopN) {
                    failures.add(fail(this, "MV_EXPAND after LIMIT is incompatible with remote ENRICH"));
                }
            });
        });

    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        if (this.mode == Mode.REMOTE) {
            checkForPlansForbiddenBeforeRemoteEnrich(failures);
        }
    }
}
