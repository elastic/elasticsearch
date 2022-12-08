/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.esql.plan.logical.ProjectReorderRenameRemove;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AnalyzerRule;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class Analyzer extends RuleExecutor<LogicalPlan> {
    private final IndexResolution indexResolution;
    private final Verifier verifier;

    private final FunctionRegistry functionRegistry;
    private final Configuration configuration;

    public Analyzer(IndexResolution indexResolution, FunctionRegistry functionRegistry, Verifier verifier, Configuration configuration) {
        assert indexResolution != null;
        this.indexResolution = indexResolution;
        this.functionRegistry = functionRegistry;
        this.verifier = verifier;
        this.configuration = configuration;
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    public LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        var resolution = new Batch("Resolution", new ResolveTable(), new ResolveRefs(), new ResolveFunctions());
        var finish = new Batch("Finish Analysis", Limiter.ONCE, new AddMissingProjection());
        return List.of(resolution, finish);
    }

    private class ResolveTable extends AnalyzerRule<UnresolvedRelation> {
        @Override
        protected LogicalPlan rule(UnresolvedRelation plan) {
            if (indexResolution.isValid() == false) {
                return plan.unresolvedMessage().equals(indexResolution.toString())
                    ? plan
                    : new UnresolvedRelation(plan.source(), plan.table(), plan.alias(), plan.frozen(), indexResolution.toString());
            }
            TableIdentifier table = plan.table();
            if (indexResolution.matches(table.index()) == false) {
                new UnresolvedRelation(
                    plan.source(),
                    plan.table(),
                    plan.alias(),
                    plan.frozen(),
                    "invalid [" + table + "] resolution to [" + indexResolution + "]"
                );
            }

            return new EsRelation(plan.source(), indexResolution.get(), plan.frozen());
        }
    }

    private static class ResolveRefs extends AnalyzerRules.BaseAnalyzerRule {

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            final List<Attribute> childrenOutput = new ArrayList<>();
            final var lazyNames = new Holder<Set<String>>();

            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }

            if (plan instanceof ProjectReorderRenameRemove p) {
                return resolveProject(p, childrenOutput);
            }

            return plan.transformExpressionsUp(UnresolvedAttribute.class, ua -> {
                if (ua.customMessage()) {
                    return ua;
                }
                Expression resolved = ua;
                var named = resolveAgainstList(ua, childrenOutput, lazyNames);
                // if resolved, return it; otherwise keep it in place to be resolved later
                if (named.size() == 1) {
                    resolved = named.get(0);
                    if (log.isTraceEnabled() && resolved.resolved()) {
                        log.trace("Resolved {} to {}", ua, resolved);
                    }
                } else {
                    if (named.size() > 0) {
                        resolved = ua.withUnresolvedMessage("Resolved [" + ua + "] unexpectedly to multiple attributes " + named);
                    }
                }
                return resolved;
            });
        }

        private LogicalPlan resolveProject(ProjectReorderRenameRemove p, List<Attribute> childOutput) {
            var lazyNames = new Holder<Set<String>>();

            List<NamedExpression> resolvedProjections = new ArrayList<>();
            var projections = p.projections();
            // start with projections

            // no projection specified or just *
            if (projections.isEmpty() || (projections.size() == 1 && projections.get(0) instanceof UnresolvedStar)) {
                resolvedProjections.addAll(childOutput);
            }
            // otherwise resolve them
            else {
                var starPosition = -1; // no star
                // resolve each item manually while paying attention to:
                // 1. name patterns a*, *b, a*b
                // 2. star * - which can only appear once and signifies "everything else" - this will be added at the end
                for (var ne : projections) {
                    if (ne instanceof UnresolvedStar) {
                        starPosition = resolvedProjections.size();
                    } else if (ne instanceof UnresolvedAttribute ua) {
                        resolvedProjections.addAll(resolveAgainstList(ua, childOutput, lazyNames));
                    } else {
                        // if this gets here it means it was already resolved
                        resolvedProjections.add(ne);
                    }
                }
                // compute star if specified and add it to the list
                if (starPosition >= 0) {
                    var remainingProjections = new ArrayList<>(childOutput);
                    remainingProjections.removeAll(resolvedProjections);
                    resolvedProjections.addAll(starPosition, remainingProjections);
                }
            }
            // continue with removals
            for (var ne : p.removals()) {
                var resolved = ne instanceof UnresolvedAttribute ua ? resolveAgainstList(ua, childOutput, lazyNames) : singletonList(ne);
                // the return list might contain either resolved elements or unresolved ones.
                // if things are resolved, remove them - if not add them to the list to trip the Verifier;
                // thus make sure to remove the intersection but add the unresolved difference (if any).
                var intersection = new ArrayList<>(resolved);
                intersection.retainAll(resolvedProjections);
                // remove things that are in common
                resolvedProjections.removeAll(intersection);
                // from both sides
                resolved.removeAll(intersection);
                // keep only the unresolved data to be picked up by the Verifier and reported further to the user
                // the resolved data that still exists until this step shouldn't anyway be considered (it's about removeable projections)
                for (var exp : resolved) {
                    if (exp instanceof UnresolvedAttribute) {
                        resolvedProjections.add(exp);
                    }
                }
            }

            return new Project(p.source(), p.child(), resolvedProjections);
        }
    }

    private static List<Attribute> resolveAgainstList(
        UnresolvedAttribute u,
        Collection<Attribute> attrList,
        Holder<Set<String>> lazyNames
    ) {
        return resolveAgainstList(u, attrList, lazyNames, false);
    }

    private static List<Attribute> resolveAgainstList(
        UnresolvedAttribute u,
        Collection<Attribute> attrList,
        Holder<Set<String>> lazyNames,
        boolean allowCompound
    ) {
        List<Attribute> matches = new ArrayList<>();

        // first take into account the qualified version
        boolean qualified = u.qualifier() != null;

        var name = u.name();
        for (Attribute attribute : attrList) {
            if (attribute.synthetic() == false) {
                boolean match = qualified ? Objects.equals(u.qualifiedName(), attribute.qualifiedName()) :
                // if the field is unqualified
                // first check the names directly
                    (Regex.simpleMatch(name, attribute.name())
                        // but also if the qualifier might not be quoted and if there's any ambiguity with nested fields
                        || Regex.simpleMatch(name, attribute.qualifiedName()));
                if (match) {
                    matches.add(attribute);
                }
            }
        }

        var isPattern = Regex.isSimpleMatchPattern(name);
        // none found - add error message
        if (matches.isEmpty()) {
            UnresolvedAttribute unresolved;
            if (isPattern) {
                unresolved = u.withUnresolvedMessage(format(null, "No match found for [{}]", name));
            } else {
                var names = lazyNames.get();
                if (names == null) {
                    names = new HashSet<>(attrList.size());
                    for (var a : attrList) {
                        String nameCandidate = a.name();
                        // add only primitives (object types would only result in another error)
                        if (DataTypes.isUnsupported(a.dataType()) == false && DataTypes.isPrimitive(a.dataType())) {
                            names.add(nameCandidate);
                        }
                    }
                    lazyNames.set(names);
                }
                unresolved = u.withUnresolvedMessage(UnresolvedAttribute.errorMessage(name, StringUtils.findSimilar(name, names)));
            }
            return singletonList(unresolved);
        }

        // found exact match or multiple if pattern
        if (matches.size() == 1 || isPattern) {
            // only add the location if the match is univocal; b/c otherwise adding the location will overwrite any preexisting one
            matches.replaceAll(e -> handleSpecialFields(u, e.withLocation(u.source()), allowCompound));
            return matches;
        }

        // report ambiguity
        List<String> refs = matches.stream().sorted((a, b) -> {
            int lineDiff = a.sourceLocation().getLineNumber() - b.sourceLocation().getLineNumber();
            int colDiff = a.sourceLocation().getColumnNumber() - b.sourceLocation().getColumnNumber();
            return lineDiff != 0 ? lineDiff : (colDiff != 0 ? colDiff : a.qualifiedName().compareTo(b.qualifiedName()));
        })
            .map(
                a -> "line "
                    + a.sourceLocation().toString().substring(1)
                    + " ["
                    + (a.qualifier() != null ? "\"" + a.qualifier() + "\".\"" + a.name() + "\"" : a.name())
                    + "]"
            )
            .collect(toList());

        return singletonList(
            u.withUnresolvedMessage(
                "Reference [" + u.qualifiedName() + "] is ambiguous (to disambiguate use quotes or qualifiers); " + "matches any of " + refs
            )
        );
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named, boolean allowCompound) {
        // if it's a object/compound type, keep it unresolved with a nice error message
        if (named instanceof FieldAttribute fa) {

            // incompatible mappings
            if (fa.field() instanceof InvalidMappedField) {
                named = u.withUnresolvedMessage(
                    "Cannot use field [" + fa.name() + "] due to ambiguities being " + ((InvalidMappedField) fa.field()).errorMessage()
                );
            }
            // unsupported types
            else if (DataTypes.isUnsupported(fa.dataType())) {
                UnsupportedEsField unsupportedField = (UnsupportedEsField) fa.field();
                if (unsupportedField.hasInherited()) {
                    named = u.withUnresolvedMessage(
                        "Cannot use field ["
                            + fa.name()
                            + "] with unsupported type ["
                            + unsupportedField.getOriginalType()
                            + "] in hierarchy (field ["
                            + unsupportedField.getInherited()
                            + "])"
                    );
                } else {
                    named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] with unsupported type [" + unsupportedField.getOriginalType() + "]"
                    );
                }
            }
            // compound fields
            else if (allowCompound == false && DataTypes.isPrimitive(fa.dataType()) == false) {
                named = u.withUnresolvedMessage(
                    "Cannot use field [" + fa.name() + "] type [" + fa.dataType().typeName() + "] only its subfields"
                );
            }
        }
        return named;
    }

    @Experimental
    private class ResolveFunctions extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan.transformExpressionsUp(UnresolvedFunction.class, uf -> {
                if (uf.analyzed()) {
                    return uf;
                }

                String name = uf.name();

                if (uf.childrenResolved() == false) {
                    return uf;
                }

                String functionName = functionRegistry.resolveAlias(name);
                if (functionRegistry.functionExists(functionName) == false) {
                    return uf.missing(functionName, functionRegistry.listFunctions());
                }
                FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                Function f = uf.buildResolved(configuration, def);
                return f;
            });
        }
    }

    private class AddMissingProjection extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            var projections = plan.collect(e -> e instanceof Project || e instanceof Aggregate);
            if (projections.isEmpty()) {
                // TODO: should unsupported fields be filtered?
                plan = new Project(plan.source(), plan, plan.output());
            }
            return plan;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }
    }
}
