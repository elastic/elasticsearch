/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Abstract parent for composite expressions like And and Or.
 */
public abstract class AbstractCompositeExpression<T> implements Expression<T> {
    private static final int MAX_COMPONENT_STRING_LENGTH = 1000;
    private static final int MAX_COMPONENTS_SIZE_FOR_TO_STRING = 10;
    private final Set<Expression<T>> components;
    private boolean simplified;
    private String toString = null;

    public AbstractCompositeExpression(Set<Expression<T>> components) {
        this.components = components;
    }

    /**
     * Transform the components of this composite expression.  Used by subclasses when implementing transform.
     * @param transformer transformer to use
     * @return result of transforming components
     */
    protected <J> Set<J> transformComponents(Expression.Transformer<T, J> transformer) {
        Set<J> builder = new HashSet<>();
        for (Expression<T> component : components) {
            builder.add(component.transform(transformer));
        }
        return Collections.unmodifiableSet(builder);
    }

    /**
     * Build an expression of this type with a list of components. Used in
     * simplification.
     */
    protected abstract AbstractCompositeExpression<T> newFrom(Set<Expression<T>> components);

    /**
     * Does this component of this expression affect the outcome of the overall
     * expression? For example the TRUE in (TRUE AND foo) doesn't effect the
     * expression and can be simplified away.
     */
    protected abstract boolean doesNotAffectOutcome(Expression<T> expression);

    /**
     * Does this component force the expression to a certain value? For example
     * the FALSE in (FALSE and foo) forces the whole expression to FALSE.
     */
    protected abstract Expression<T> componentForcesOutcome(Expression<T> expression);

    protected abstract String toStringJoiner();

    @Override
    public boolean alwaysFalse() {
        return false;
    }

    @Override
    public boolean alwaysTrue() {
        return false;
    }

    @Override
    public Expression<T> simplify() {
        if (simplified) {
            return this;
        }
        Iterator<Expression<T>> componentsItr = components.iterator();
        List<Expression<T>> newComponentsBuilder = null;
        boolean changed = false;
        while (componentsItr.hasNext()) {
            Expression<T> expression = componentsItr.next();
            Expression<T> simplified = expression.simplify();
            changed |= expression != simplified;
            if (doesNotAffectOutcome(simplified)) {
                changed |= true;
                continue;
            }
            Expression<T> forcedOutcome = componentForcesOutcome(simplified);
            if (forcedOutcome != null) {
                return forcedOutcome;
            }
            if (newComponentsBuilder == null) {
                newComponentsBuilder = new ArrayList<>(components.size());
            }
            if (simplified.getClass() == getClass()) {
                changed |= true;
                newComponentsBuilder.addAll(((AbstractCompositeExpression<T>) simplified).components);
                continue;
            }
            newComponentsBuilder.add(simplified);
        }
        if (newComponentsBuilder == null) {
            // Nothing left in the expression!
            return True.instance();
        }
        switch (newComponentsBuilder.size()) {
        case 0:
            return True.instance();
        case 1:
            return newComponentsBuilder.get(0);
        default:
        }
        Expression<T> commonExtracted = extractCommon(changed ? newComponentsBuilder : components);
        if (commonExtracted != null) {
            return commonExtracted;
        }

        if (!changed) {
            this.simplified = true;
            return this;
        }
        AbstractCompositeExpression<T> result = newFrom(changed ? Set.copyOf(newComponentsBuilder) : components);
        result.simplified = true;
        return result;
    }

    private Expression<T> extractCommon(Iterable<Expression<T>> newComponents) {
        // Are all composite subexpressions of the same type?
        boolean allCompositesOfSameType = true;
        // Are all the non-composite subexpressions of this type the same?
        boolean nonCompositesAreSame = true;
        // First non-composite subexpression.
        Expression<T> firstNonComposite = null;
        // If all composite subexpressions are of the same type then what is that type?
        Class<?> compositesClass = null;
        for (Expression<T> current : newComponents) {
            if (current.isComposite()) {
                if (compositesClass == null) {
                    compositesClass = current.getClass();
                } else {
                    allCompositesOfSameType &= compositesClass == current.getClass();
                }
            } else {
                if (firstNonComposite == null) {
                    firstNonComposite = current;
                } else {
                    nonCompositesAreSame &= firstNonComposite.equals(current);
                }
            }
        }

        if (firstNonComposite == null) {
            // Everything is composite and they are all of the same type.
            if (allCompositesOfSameType) {
                // If this component is composed of composite components we can
                // attempt to factor out any equivalent parts
                AbstractCompositeExpression<T> first = (AbstractCompositeExpression<T>) newComponents.iterator().next();
                Set<Expression<T>> sharedComponents = null;
                for (Expression<T> component : first.components) {
                    boolean shared = true;
                    Iterator<Expression<T>> current = newComponents.iterator();
                    while (shared && current.hasNext()) {
                        shared &= ((AbstractCompositeExpression<T>) current.next()).components.contains(component);
                    }
                    if (shared) {
                        if (sharedComponents == null) {
                            sharedComponents = new HashSet<>();
                        }
                        sharedComponents.add(component);
                    }
                }
                if (sharedComponents != null) {
                    // Build all the subcomponents with the common part extracted
                    Set<Expression<T>> extractedComponents = new HashSet<>();
                    AbstractCompositeExpression<T> composite = null;
                    for (Expression<T> component : newComponents) {
                        composite = (AbstractCompositeExpression<T>) component;
                        extractedComponents.add(composite.newFrom(Set.copyOf(Sets.difference(composite.components, sharedComponents)))
                                .simplify());
                    }
                    sharedComponents.add(newFrom(Collections.unmodifiableSet(extractedComponents)).simplify());
                    return composite.newFrom(Collections.unmodifiableSet(sharedComponents)).simplify();
                }
            }
        } else {
            if (allCompositesOfSameType && nonCompositesAreSame && canFactorOut(newComponents, firstNonComposite)) {
                Set<Expression<T>> sharedComponents = new HashSet<>();
                sharedComponents.add(firstNonComposite);
                AbstractCompositeExpression<T> composite = null;
                Set<Expression<T>> extractedComponents = new HashSet<>();
                for (Expression<T> component : newComponents) {
                    if (!component.isComposite()) {
                        continue;
                    }
                    composite = (AbstractCompositeExpression<T>) component;
                    extractedComponents.add(composite.newFrom(Collections.unmodifiableSet((
                            Sets.difference(composite.components, sharedComponents))))
                            .simplify());
                }
                // In the rare case that there aren't any composites but all the
                // non-composits are the same we should just return that
                // non-composite
                if (composite == null) {
                    return firstNonComposite;
                }
                // Add True to represent the extracted common component
                extractedComponents.add(True.<T>instance());
                sharedComponents.add(newFrom(Collections.unmodifiableSet(extractedComponents)).simplify());
                return composite.newFrom(Collections.unmodifiableSet(sharedComponents)).simplify();
            }
        }
        return null;
    }
    
    /**
     * Can we factor commonComposite out of all subexpressions?
     */
    private boolean canFactorOut(Iterable<Expression<T>> subexpressions, Expression<T> commonComposite) {
        for (Expression<T> current : subexpressions) {
            if (!current.equals(commonComposite)) {
                if (!current.isComposite()) {
                    return false;
                }
                AbstractCompositeExpression<T> composite = (AbstractCompositeExpression<T>) current;
                if (!composite.components.contains(commonComposite)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean isComposite() {
        return true;
    }

    @Override
    public String toString() {
        if (toString != null) {
            return toString;
        }
        if (components.size() > MAX_COMPONENTS_SIZE_FOR_TO_STRING) {
            toString = "(lots of " + toStringJoiner() + "s)";
            return toString;
        }
        StringBuilder b = new StringBuilder();
        b.append('(');
        boolean first = true;
        for (Expression<T> component: components) {
            if (first) {
                first = false;
            } else {
                b.append(toStringJoiner());
            }
            b.append(component.toString());
        }
        b.append(')');
        if (b.length() > MAX_COMPONENT_STRING_LENGTH) {
            toString = "TOO_BIG";
        } else {
            toString = b.toString();
        }
        return toString;
    }

    @Override
    public int hashCode() {
        return components.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        AbstractCompositeExpression other = (AbstractCompositeExpression) obj;
        return components.equals(other.components);
    }
}
