/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.TypedParamValue;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.ql.analyzer.TableInfo;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.MetadataAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.MappingException;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.index.IndexResolver.UNMAPPED;
import static org.elasticsearch.xpack.ql.util.ActionListeners.map;
import static org.elasticsearch.xpack.ql.util.StringUtils.WILDCARD;

public class EsqlSession {

    private static final Logger LOGGER = LogManager.getLogger(EsqlSession.class);

    private final String sessionId;
    private final EsqlConfiguration configuration;
    private final IndexResolver indexResolver;
    private final EsqlIndexResolver esqlIndexResolver;
    private final EnrichPolicyResolver enrichPolicyResolver;

    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final FunctionRegistry functionRegistry;
    private final LogicalPlanOptimizer logicalPlanOptimizer;

    private final Mapper mapper;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;

    public EsqlSession(
        String sessionId,
        EsqlConfiguration configuration,
        IndexResolver indexResolver,
        EsqlIndexResolver esqlIndexResolver,
        EnrichPolicyResolver enrichPolicyResolver,
        PreAnalyzer preAnalyzer,
        FunctionRegistry functionRegistry,
        LogicalPlanOptimizer logicalPlanOptimizer,
        Mapper mapper,
        Verifier verifier
    ) {
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.indexResolver = indexResolver;
        this.esqlIndexResolver = esqlIndexResolver;
        this.enrichPolicyResolver = enrichPolicyResolver;
        this.preAnalyzer = preAnalyzer;
        this.verifier = verifier;
        this.functionRegistry = functionRegistry;
        this.mapper = mapper;
        this.logicalPlanOptimizer = logicalPlanOptimizer;
        this.physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
    }

    public String sessionId() {
        return sessionId;
    }

    public void execute(EsqlQueryRequest request, ActionListener<PhysicalPlan> listener) {
        LOGGER.debug("ESQL query:\n{}", request.query());
        optimizedPhysicalPlan(
            parse(request.query(), request.params()),
            listener.map(plan -> EstimatesRowSize.estimateRowSize(0, plan.transformUp(FragmentExec.class, f -> {
                QueryBuilder filter = request.filter();
                if (filter != null) {
                    var fragmentFilter = f.esFilter();
                    // TODO: have an ESFilter and push down to EsQueryExec / EsSource
                    // This is an ugly hack to push the filter parameter to Lucene
                    // TODO: filter integration testing
                    filter = fragmentFilter != null ? boolQuery().filter(fragmentFilter).must(filter) : filter;
                    LOGGER.debug("Fold filter {} to EsQueryExec", filter);
                    f = new FragmentExec(f.source(), f.fragment(), filter, f.estimatedRowSize());
                }
                return f;
            })))
        );
    }

    private LogicalPlan parse(String query, List<TypedParamValue> params) {
        var parsed = new EsqlParser().createStatement(query, params);
        LOGGER.debug("Parsed logical plan:\n{}", parsed);
        return parsed;
    }

    public void analyzedPlan(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, (indices, policies) -> {
            Analyzer analyzer = new Analyzer(new AnalyzerContext(configuration, functionRegistry, indices, policies), verifier);
            var plan = analyzer.analyze(parsed);
            LOGGER.debug("Analyzed plan:\n{}", plan);
            return plan;
        }, listener);
    }

    private <T> void preAnalyze(LogicalPlan parsed, BiFunction<IndexResolution, EnrichResolution, T> action, ActionListener<T> listener) {
        PreAnalyzer.PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);
        var unresolvedPolicies = preAnalysis.enriches.stream()
            .map(e -> new EnrichPolicyResolver.UnresolvedPolicy((String) e.policyName().fold(), e.mode()))
            .collect(Collectors.toSet());
        final Set<String> targetClusters = enrichPolicyResolver.groupIndicesPerCluster(
            preAnalysis.indices.stream()
                .flatMap(t -> Arrays.stream(Strings.commaDelimitedListToStringArray(t.id().index())))
                .toArray(String[]::new)
        ).keySet();
        enrichPolicyResolver.resolvePolicies(targetClusters, unresolvedPolicies, listener.delegateFailureAndWrap((l, enrichResolution) -> {
            // first we need the match_fields names from enrich policies and THEN, with an updated list of fields, we call field_caps API
            var matchFields = enrichResolution.resolvedEnrichPolicies()
                .stream()
                .map(ResolvedEnrichPolicy::matchField)
                .collect(Collectors.toSet());
            preAnalyzeIndices(parsed, l.delegateFailureAndWrap((ll, indexResolution) -> {
                if (indexResolution.isValid()) {
                    Set<String> newClusters = enrichPolicyResolver.groupIndicesPerCluster(
                        indexResolution.get().concreteIndices().toArray(String[]::new)
                    ).keySet();
                    // If new clusters appear when resolving the main indices, we need to resolve the enrich policies again
                    // or exclude main concrete indices. Since this is rare, it's simpler to resolve the enrich policies again.
                    // TODO: add a test for this
                    if (targetClusters.containsAll(newClusters) == false) {
                        enrichPolicyResolver.resolvePolicies(
                            newClusters,
                            unresolvedPolicies,
                            ll.map(newEnrichResolution -> action.apply(indexResolution, newEnrichResolution))
                        );
                        return;
                    }
                }
                ll.onResponse(action.apply(indexResolution, enrichResolution));
            }), matchFields);
        }));
    }

    private <T> void preAnalyzeIndices(LogicalPlan parsed, ActionListener<IndexResolution> listener, Set<String> enrichPolicyMatchFields) {
        PreAnalyzer.PreAnalysis preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        if (preAnalysis.indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (preAnalysis.indices.size() == 1) {
            TableInfo tableInfo = preAnalysis.indices.get(0);
            TableIdentifier table = tableInfo.id();
            var fieldNames = fieldNames(parsed, enrichPolicyMatchFields);

            if (Assertions.ENABLED) {
                resolveMergedMappingAgainstBothResolvers(table.index(), fieldNames, listener);
            } else {
                esqlIndexResolver.resolveAsMergedMapping(table.index(), fieldNames, listener);
            }
        } else {
            try {
                // occurs when dealing with local relations (row a = 1)
                listener.onResponse(IndexResolution.invalid("[none specified]"));
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

    /**
     * Resolves the mapping against both the new, fast {@link #esqlIndexResolver}
     * and the older, known correct {@link #indexResolver}. We then assert that they
     * produce the same output.
     */
    private void resolveMergedMappingAgainstBothResolvers(
        String indexWildcard,
        Set<String> fieldNames,
        ActionListener<IndexResolution> listener
    ) {
        indexResolver.resolveAsMergedMapping(indexWildcard, fieldNames, false, Map.of(), new ActionListener<>() {
            @Override
            public void onResponse(IndexResolution fromQl) {
                esqlIndexResolver.resolveAsMergedMapping(indexWildcard, fieldNames, new ActionListener<>() {
                    @Override
                    public void onResponse(IndexResolution fromEsql) {
                        if (fromQl.isValid() == false) {
                            if (fromEsql.isValid()) {
                                throw new IllegalArgumentException(
                                    "ql and esql didn't make the same resolution: validity differs " + fromQl + " != " + fromEsql
                                );
                            }
                        } else {
                            assertSameMappings("", fromQl.get().mapping(), fromEsql.get().mapping());
                            if (fromQl.get().concreteIndices().equals(fromEsql.get().concreteIndices()) == false) {
                                throw new IllegalArgumentException(
                                    "ql and esql didn't make the same resolution: concrete indices differ "
                                        + fromQl.get().concreteIndices()
                                        + " != "
                                        + fromEsql.get().concreteIndices()
                                );
                            }
                        }
                        listener.onResponse(fromEsql);
                    }

                    private void assertSameMappings(String prefix, Map<String, EsField> fromQl, Map<String, EsField> fromEsql) {
                        List<String> qlFields = new ArrayList<>();
                        qlFields.addAll(fromQl.keySet());
                        Collections.sort(qlFields);

                        List<String> esqlFields = new ArrayList<>();
                        esqlFields.addAll(fromEsql.keySet());
                        Collections.sort(esqlFields);
                        if (qlFields.equals(esqlFields) == false) {
                            throw new IllegalArgumentException(
                                prefix + ": ql and esql didn't make the same resolution: fields differ \n" + qlFields + " !=\n" + esqlFields
                            );
                        }

                        for (int f = 0; f < qlFields.size(); f++) {
                            String name = qlFields.get(f);
                            EsField qlField = fromQl.get(name);
                            EsField esqlField = fromEsql.get(name);

                            if (qlField.getProperties().isEmpty() == false || esqlField.getProperties().isEmpty() == false) {
                                assertSameMappings(
                                    prefix.equals("") ? name : prefix + "." + name,
                                    qlField.getProperties(),
                                    esqlField.getProperties()
                                );
                            }

                            /*
                             * Check that the field itself is the same, skipping isAlias because
                             * we don't actually use it in ESQL and the EsqlIndexResolver doesn't
                             * produce exactly the same result.
                             */
                            if (qlField.getDataType().equals(DataTypes.UNSUPPORTED) == false
                                && qlField.getName().equals(esqlField.getName()) == false
                            // QL uses full paths for unsupported fields. ESQL does not. This particular difference is fine.
                            ) {
                                throw new IllegalArgumentException(
                                    prefix
                                        + "."
                                        + name
                                        + ": ql and esql didn't make the same resolution: names differ ["
                                        + qlField.getName()
                                        + "] != ["
                                        + esqlField.getName()
                                        + "]"
                                );
                            }
                            if (qlField.getDataType() != esqlField.getDataType()) {
                                throw new IllegalArgumentException(
                                    prefix
                                        + "."
                                        + name
                                        + ": ql and esql didn't make the same resolution: types differ ["
                                        + qlField.getDataType()
                                        + "] != ["
                                        + esqlField.getDataType()
                                        + "]"
                                );
                            }
                            if (qlField.isAggregatable() != esqlField.isAggregatable()) {
                                throw new IllegalArgumentException(
                                    prefix
                                        + "."
                                        + name
                                        + ": ql and esql didn't make the same resolution: aggregability differ ["
                                        + qlField.isAggregatable()
                                        + "] != ["
                                        + esqlField.isAggregatable()
                                        + "]"
                                );
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        },
            EsqlSession::specificValidity,
            IndexResolver.PRESERVE_PROPERTIES,
            // TODO no matter what metadata fields are asked in a query, the "allowedMetadataFields" is always _index, does it make
            // sense to reflect the actual list of metadata fields instead?
            IndexResolver.INDEX_METADATA_FIELD
        );
    }

    static Set<String> fieldNames(LogicalPlan parsed, Set<String> enrichPolicyMatchFields) {
        if (false == parsed.anyMatch(plan -> plan instanceof Aggregate || plan instanceof Project)) {
            // no explicit columns selection, for example "from employees"
            return IndexResolver.ALL_FIELDS;
        }

        Holder<Boolean> projectAll = new Holder<>(false);
        parsed.forEachExpressionDown(UnresolvedStar.class, us -> {// explicit "*" fields selection
            if (projectAll.get()) {
                return;
            }
            projectAll.set(true);
        });
        if (projectAll.get()) {
            return IndexResolver.ALL_FIELDS;
        }

        AttributeSet references = new AttributeSet();
        // "keep" attributes are special whenever a wildcard is used in their name
        // ie "from test | eval lang = languages + 1 | keep *l" should consider both "languages" and "*l" as valid fields to ask for
        AttributeSet keepCommandReferences = new AttributeSet();
        List<Predicate<String>> keepMatches = new ArrayList<>();
        List<String> keepPatterns = new ArrayList<>();

        parsed.forEachDown(p -> {// go over each plan top-down
            if (p instanceof RegexExtract re) { // for Grok and Dissect
                AttributeSet dissectRefs = p.references();
                // don't add to the list of fields the extracted ones (they are not real fields in mappings)
                dissectRefs.removeAll(re.extractedFields());
                references.addAll(dissectRefs);
                // also remove other down-the-tree references to the extracted fields
                for (Attribute extracted : re.extractedFields()) {
                    references.removeIf(attr -> matchByName(attr, extracted.qualifiedName(), false));
                }
            } else if (p instanceof Enrich) {
                AttributeSet enrichRefs = p.references();
                // Enrich adds an EmptyAttribute if no match field is specified
                // The exact name of the field will be added later as part of enrichPolicyMatchFields Set
                enrichRefs.removeIf(attr -> attr instanceof EmptyAttribute);
                references.addAll(enrichRefs);
            } else {
                references.addAll(p.references());
                // special handling for UnresolvedPattern (which is not an UnresolvedAttribute)
                p.forEachExpression(UnresolvedNamePattern.class, up -> {
                    var ua = new UnresolvedAttribute(up.source(), up.name());
                    references.add(ua);
                    if (p instanceof Keep) {
                        keepCommandReferences.add(ua);
                        keepMatches.add(up::match);
                    }
                });
                if (p instanceof Keep) {
                    keepCommandReferences.addAll(p.references());
                }
            }

            // remove any already discovered UnresolvedAttributes that are in fact aliases defined later down in the tree
            // for example "from test | eval x = salary | stats max = max(x) by gender"
            // remove the UnresolvedAttribute "x", since that is an Alias defined in "eval"
            p.forEachExpressionDown(Alias.class, alias -> {
                // do not remove the UnresolvedAttribute that has the same name as its alias, ie "rename id = id"
                // or the UnresolvedAttributes that are used in Functions that have aliases "STATS id = MAX(id)"
                if (p.references().names().contains(alias.qualifiedName())) {
                    return;
                }
                references.removeIf(attr -> matchByName(attr, alias.qualifiedName(), keepCommandReferences.contains(attr)));
            });
        });

        // remove valid metadata attributes because they will be filtered out by the IndexResolver anyway
        // otherwise, in some edge cases, we will fail to ask for "*" (all fields) instead
        references.removeIf(a -> a instanceof MetadataAttribute || MetadataAttribute.isSupported(a.qualifiedName()));
        Set<String> fieldNames = references.names();

        if (fieldNames.isEmpty() && enrichPolicyMatchFields.isEmpty()) {
            // there cannot be an empty list of fields, we'll ask the simplest and lightest one instead: _index
            return IndexResolver.INDEX_METADATA_FIELD;
        } else {
            fieldNames.addAll(subfields(fieldNames));
            fieldNames.addAll(enrichPolicyMatchFields);
            fieldNames.addAll(subfields(enrichPolicyMatchFields));
            return fieldNames;
        }
    }

    private static boolean matchByName(Attribute attr, String other, boolean skipIfPattern) {
        boolean isPattern = Regex.isSimpleMatchPattern(attr.qualifiedName());
        if (skipIfPattern && isPattern) {
            return false;
        }
        var name = attr.qualifiedName();
        return isPattern ? Regex.simpleMatch(name, other) : name.equals(other);
    }

    private static Set<String> subfields(Set<String> names) {
        return names.stream().filter(name -> name.endsWith(WILDCARD) == false).map(name -> name + ".*").collect(Collectors.toSet());
    }

    public void optimizedPlan(LogicalPlan logicalPlan, ActionListener<LogicalPlan> listener) {
        analyzedPlan(logicalPlan, map(listener, p -> {
            var plan = logicalPlanOptimizer.optimize(p);
            LOGGER.debug("Optimized logicalPlan plan:\n{}", plan);
            return plan;
        }));
    }

    public void physicalPlan(LogicalPlan optimized, ActionListener<PhysicalPlan> listener) {
        optimizedPlan(optimized, map(listener, p -> {
            var plan = mapper.map(p);
            LOGGER.debug("Physical plan:\n{}", plan);
            return plan;
        }));
    }

    public void optimizedPhysicalPlan(LogicalPlan logicalPlan, ActionListener<PhysicalPlan> listener) {
        physicalPlan(logicalPlan, map(listener, p -> {
            var plan = physicalPlanOptimizer.optimize(p);
            LOGGER.debug("Optimized physical plan:\n{}", plan);
            return plan;
        }));
    }

    public static InvalidMappedField specificValidity(String fieldName, Map<String, FieldCapabilities> types) {
        boolean hasUnmapped = types.containsKey(UNMAPPED);
        boolean hasTypeConflicts = types.size() > (hasUnmapped ? 2 : 1);
        String metricConflictsTypeName = null;
        boolean hasMetricConflicts = false;

        if (hasTypeConflicts == false) {
            for (Map.Entry<String, FieldCapabilities> type : types.entrySet()) {
                if (UNMAPPED.equals(type.getKey())) {
                    continue;
                }
                if (type.getValue().metricConflictsIndices() != null && type.getValue().metricConflictsIndices().length > 0) {
                    hasMetricConflicts = true;
                    metricConflictsTypeName = type.getKey();
                    break;
                }
            }
        }

        InvalidMappedField result = null;
        if (hasMetricConflicts) {
            StringBuilder errorMessage = new StringBuilder();
            errorMessage.append(
                "mapped as different metric types in indices: ["
                    + String.join(", ", types.get(metricConflictsTypeName).metricConflictsIndices())
                    + "]"
            );
            result = new InvalidMappedField(fieldName, errorMessage.toString());
        }
        return result;
    };
}
