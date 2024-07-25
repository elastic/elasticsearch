/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Enrich extends UnaryPlan implements GeneratingPlan<Enrich> {
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
                newEnrichFields.add(new Alias(ra.source(), newName, ra.qualifier(), ra, new NameId(), ra.synthetic()));
            } else if (enrichField instanceof Alias a) {
                newEnrichFields.add(new Alias(a.source(), newName, a.qualifier(), a.child(), new NameId(), a.synthetic()));
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
}
