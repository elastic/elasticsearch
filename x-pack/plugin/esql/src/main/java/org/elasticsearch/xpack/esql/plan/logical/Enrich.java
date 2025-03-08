/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Enrich extends UnaryPlan implements GeneratingPlan<Enrich>, PostAnalysisPlanVerificationAware, TelemetryAware, SortAgnostic {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Enrich",
        Enrich::readFrom
    );

    private final Expression policyName;
    private final NamedExpression matchField;
    private final EnrichPolicy policy;
    private final Map<String, String> concreteIndices; // cluster -> enrich indices
    // This could be simplified by just always using an Alias.
    private final List<NamedExpression> enrichFields;
    private List<Attribute> output;

    private final Mode mode;

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
        Enrich.Mode mode = Enrich.Mode.ANY;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            mode = in.readEnum(Enrich.Mode.class);
        }
        final Source source = Source.readFrom((PlanStreamInput) in);
        final LogicalPlan child = in.readNamedWriteable(LogicalPlan.class);
        final Expression policyName = in.readNamedWriteable(Expression.class);
        final NamedExpression matchField = in.readNamedWriteable(NamedExpression.class);
        if (in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            in.readString(); // discard the old policy name
        }
        final EnrichPolicy policy = new EnrichPolicy(in);
        final Map<String, String> concreteIndices;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            concreteIndices = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            EsIndex esIndex = EsIndex.readFrom(in);
            if (esIndex.concreteIndices().size() > 1) {
                throw new IllegalStateException("expected a single enrich index; got " + esIndex);
            }
            concreteIndices = Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Iterables.get(esIndex.concreteIndices(), 0));
        }
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeEnum(mode());
        }

        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(policyName());
        out.writeNamedWriteable(matchField());
        if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            out.writeString(BytesRefs.toString(policyName().fold(FoldContext.small() /* TODO remove me */))); // old policy name
        }
        policy().writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeMap(concreteIndices(), StreamOutput::writeString, StreamOutput::writeString);
        } else {
            Map<String, String> concreteIndices = concreteIndices();
            if (concreteIndices.keySet().equals(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))) {
                String enrichIndex = concreteIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                EsIndex esIndex = new EsIndex(enrichIndex, Map.of(), Map.of(enrichIndex, IndexMode.STANDARD));
                esIndex.writeTo(out);
            } else {
                throw new IllegalStateException("expected a single enrich index; got " + concreteIndices);
            }
        }
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

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return Enrich::checkRemoteEnrich;
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
     * retaining the originating cluster and restructing pages for routing, which might be complicated.
     */
    private static void checkRemoteEnrich(LogicalPlan plan, Failures failures) {
        boolean[] agg = { false };
        boolean[] enrichCoord = { false };

        plan.forEachUp(UnaryPlan.class, u -> {
            if (u instanceof Aggregate) {
                agg[0] = true;
            } else if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                enrichCoord[0] = true;
            }
            if (u instanceof Enrich enrich && enrich.mode() == Enrich.Mode.REMOTE) {
                if (agg[0]) {
                    failures.add(fail(enrich, "ENRICH with remote policy can't be executed after STATS"));
                }
                if (enrichCoord[0]) {
                    failures.add(fail(enrich, "ENRICH with remote policy can't be executed after another ENRICH with coordinator policy"));
                }
            }
        });
    }
}
