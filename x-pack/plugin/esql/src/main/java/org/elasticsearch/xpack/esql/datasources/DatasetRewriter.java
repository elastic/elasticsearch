/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites {@code FROM <dataset>} into the same {@link UnresolvedExternalRelation} the
 * {@code EXTERNAL} command produces, so both paths converge at the existing resolver + analyzer.
 * Runs once on the parsed plan before pre-analysis.
 *
 * <p>Pattern expansion (wildcards, exclusions, date math, hidden flag) is delegated to
 * {@link IndexAbstractionResolver} with {@code resolveDatasets(true)} — the same machinery
 * {@code FROM <index>} uses, so dataset names participate in the standard FROM syntax without a
 * parallel resolution path.
 */
public final class DatasetRewriter {

    private static final Logger logger = LogManager.getLogger(DatasetRewriter.class);

    /**
     * Built from {@link IndexResolver#DEFAULT_OPTIONS}; only delta is {@code resolveDatasets(true)}. Shared with
     * {@link org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction.Request} so the security filter resolves the
     * same patterns with the same semantics this rewriter then applies. {@code ALLOW_UNAVAILABLE_TARGETS} is what makes
     * an unauthorized explicit dataset name indistinguishable from a missing one (filtered, not 403) — mirroring how
     * unauthorized indices and views behave in FROM.
     */
    public static final IndicesOptions RESOLVER_OPTIONS = IndicesOptions.builder(IndexResolver.DEFAULT_OPTIONS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(false).build())
        .build();

    private DatasetRewriter() {}

    /**
     * The FROM patterns of {@code parsed} that could resolve to a registered dataset — the patterns that must be
     * read-authorized via {@code EsqlResolveDatasetAction} before {@link #rewrite} strips the dataset names from the
     * plan. Empty means no dataset can be involved and the authorization round-trip can be skipped entirely (the
     * common, no-datasets path). Applies the same short-circuits {@link #rewrite} does: no project metadata, no
     * registered datasets (the feature-flag off-switch), remote patterns (datasets are local-only; CCS relations are
     * never rewritten).
     */
    static List<String> candidatePatterns(LogicalPlan parsed, ProjectMetadata projectMetadata) {
        if (projectMetadata == null) {
            return List.of();
        }
        Set<String> datasetNames = DatasetMetadata.get(projectMetadata).datasets().keySet();
        if (datasetNames.isEmpty()) {
            return List.of();
        }
        // LinkedHashSet: dedup across relations while preserving pattern order within each relation, so
        // exclusion semantics ("ds*,-ds_foo") survive the union. Relations are concatenated; an exclusion
        // from one relation can in principle shadow an inclusion from another, but FROM lists mixing the
        // same dataset positively and negatively across subplans have no defined precedence anyway.
        Set<String> candidates = new LinkedHashSet<>();
        parsed.forEachUp(UnresolvedRelation.class, r -> {
            List<String> patterns = Arrays.asList(Strings.splitStringByCommaToArray(r.indexPattern().indexPattern()));
            for (String pattern : patterns) {
                if (RemoteClusterAware.isRemoteIndexName(pattern)) {
                    return;
                }
            }
            if (anyPatternCouldMatchDataset(patterns, datasetNames)) {
                candidates.addAll(patterns);
            }
        });
        return List.copyOf(candidates);
    }

    /**
     * Registered dataset name → parent datasource name, for {@code EsqlResolveDatasetAction.Request}. The full
     * registered map, not just pattern matches: the request derives {@code dataSourceNames()} from the intersection
     * with its post-resolution indices, so extra entries are never consulted, and a static pattern pre-match here
     * could under-approximate (date math) and silently skip the datasource check.
     */
    static Map<String, String> datasetToDataSourceMap(ProjectMetadata projectMetadata) {
        Map<String, String> map = new HashMap<>();
        for (var entry : DatasetMetadata.get(projectMetadata).datasets().entrySet()) {
            map.put(entry.getKey(), entry.getValue().dataSource().getName());
        }
        return map;
    }

    /**
     * Walks {@code parsed} and rewrites every {@link UnresolvedRelation} whose pattern resolves to
     * dataset(s) into {@link UnresolvedExternalRelation} (single dataset) or {@link UnionAll} of
     * such (multi). All other relations are left untouched. Two short-circuits avoid resolver
     * cost on the common path: {@code projectMetadata == null}, or no datasets registered — and
     * since {@link DatasetMetadata#ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG} gates the CRUD layer
     * that puts datasets into cluster state, the no-datasets check is the natural off-switch.
     *
     * <p>Throws {@link VerificationException} for: heterogeneous FROM (datasets + non-datasets),
     * non-{@code STANDARD} {@link IndexMode} on a dataset, METADATA fields on a dataset, or
     * {@code UnionAll} branch-cap exceeded. Designed to run once on the parsed plan before
     * pre-analysis (so the analyzer sees a uniform {@code UnresolvedExternalRelation} tree
     * regardless of whether the user wrote {@code FROM <dataset>} or inline {@code EXTERNAL}).
     *
     * @param authorizedDatasets the dataset names the principal may read, from
     *                           {@code EsqlResolveDatasetAction} (on an unsecured cluster: all that resolve). A dataset
     *                           outside this set is treated as nonexistent — skipped under a wildcard, and rejected
     *                           with the standard {@code Unknown index} error when named explicitly — mirroring how
     *                           security hides unauthorized indices and views from FROM.
     */
    static LogicalPlan rewrite(
        LogicalPlan parsed,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver iner,
        Set<String> authorizedDatasets
    ) {
        if (projectMetadata == null) {
            return parsed;
        }
        DatasetMetadata datasetMetadata = DatasetMetadata.get(projectMetadata);
        if (datasetMetadata.datasets().isEmpty()) {
            return parsed;
        }
        DataSourceMetadata dataSourceMetadata = DataSourceMetadata.get(projectMetadata);
        IndexAbstractionResolver resolver = new IndexAbstractionResolver(iner);
        return parsed.transformUp(
            UnresolvedRelation.class,
            r -> rewriteOne(r, projectMetadata, datasetMetadata, dataSourceMetadata, resolver, authorizedDatasets)
        );
    }

    private static LogicalPlan rewriteOne(
        UnresolvedRelation relation,
        ProjectMetadata projectMetadata,
        DatasetMetadata datasets,
        DataSourceMetadata dataSources,
        IndexAbstractionResolver resolver,
        Set<String> authorizedDatasets
    ) {
        List<String> patterns = Arrays.asList(Strings.splitStringByCommaToArray(relation.indexPattern().indexPattern()));

        // Datasets are local-only; cluster-prefixed patterns skip rewriting so CCS sees the original FROM.
        for (String pattern : patterns) {
            if (RemoteClusterAware.isRemoteIndexName(pattern)) {
                return relation;
            }
        }

        // Skip the O(indices) resolver expansion when no pattern could match a registered dataset name.
        if (anyPatternCouldMatchDataset(patterns, datasets.datasets().keySet()) == false) {
            return relation;
        }

        Map<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        // Expansion is name-matching only — the read authorization happened in EsqlResolveDatasetAction
        // (whose result is authorizedDatasets) and is enforced in the classification loop below, where
        // explicit-vs-wildcard provenance is still known.
        var resolved = resolver.resolveIndexAbstractions(
            patterns,
            RESOLVER_OPTIONS,
            projectMetadata,
            componentSelector -> indicesLookup.keySet(),
            (name, selector) -> true,
            true
        );

        // Explicitly named targets (positive, non-wildcard, date math resolved): an unauthorized dataset
        // named explicitly must error like a missing index, not silently drop from a multi-target FROM.
        Set<String> explicitNames = new LinkedHashSet<>();
        for (String pattern : patterns) {
            if (pattern.isEmpty() || pattern.charAt(0) == '-' || Regex.isSimpleMatchPattern(pattern)) {
                continue;
            }
            explicitNames.add(IndexNameExpressionResolver.resolveDateMathExpression(pattern));
        }

        List<String> datasetNames = new ArrayList<>();
        List<String> nonDatasetNames = new ArrayList<>();
        for (String name : resolved.getLocalIndicesList()) {
            IndexAbstraction abs = indicesLookup.get(name);
            if (abs == null) {
                // Synthesized name (e.g. date math under ALLOW_UNAVAILABLE_TARGETS) — neither dataset
                // nor real non-dataset; skipping doesn't suppress mixed-FROM rejection.
                continue;
            }
            if (abs.getType() == IndexAbstraction.Type.DATASET) {
                if (authorizedDatasets.contains(name)) {
                    datasetNames.add(name);
                } else if (explicitNames.contains(name)) {
                    // Same error an unauthorized (or missing) index/view produces — no existence oracle.
                    throw new VerificationException("Unknown index [" + name + "]");
                }
                // else: matched by a wildcard the principal can't read it under — invisible, like an
                // unauthorized index in wildcard expansion.
            } else {
                nonDatasetNames.add(name);
            }
        }

        if (datasetNames.isEmpty()) {
            return relation;
        }
        if (relation.indexMode() != null && relation.indexMode() != IndexMode.STANDARD) {
            String message = switch (relation.indexMode()) {
                case TIME_SERIES -> "TS command is not supported for datasets; dataset(s) requested: " + datasetNames;
                case LOOKUP -> "LOOKUP JOIN against a dataset is not supported; dataset(s) requested: " + datasetNames;
                case LOGSDB -> "LOGSDB index mode on FROM <dataset> is not supported; dataset(s) requested: " + datasetNames;
                default -> "FROM <dataset> with index mode ["
                    + relation.indexMode().getName()
                    + "] is not supported; dataset(s) requested: "
                    + datasetNames;
            };
            throw new VerificationException(message);
        }
        if (relation.metadataFields().isEmpty() == false) {
            // Reject rather than silently drop. _index synthesis on datasets is tracked separately
            // (proposed: dataset name as _index); _id/_source/_score have no agreed semantics yet.
            throw new VerificationException("METADATA fields are not supported on datasets; dataset(s) requested: " + datasetNames);
        }
        if (nonDatasetNames.isEmpty() == false) {
            // Counts only in the user-facing message (names may be unreadable to the caller); full
            // names go to DEBUG for operator triage. Rejection removed by heterogeneous FROM.
            logger.debug(
                "DatasetRewriter rejecting mixed FROM: pattern=[{}] datasets={} non-datasets={}",
                relation.indexPattern().indexPattern(),
                datasetNames,
                nonDatasetNames
            );
            throw new VerificationException(
                "FROM mixing datasets and non-datasets is not supported; requested mix: "
                    + nonDatasetNames.size()
                    + " non-dataset(s) and "
                    + datasetNames.size()
                    + " dataset(s)"
            );
        }
        List<LogicalPlan> children = new ArrayList<>(datasetNames.size());
        for (String name : datasetNames) {
            Dataset dataset = datasets.get(name);
            DataSource parent = dataSources.get(dataset.dataSource().getName());
            // DataSourceService.deleteDataSources rejects (409) on orphans, so a null parent here
            // means a broken-invariant state (e.g. corrupt cluster-state restore) — throw with context.
            if (parent == null) {
                throw new IllegalStateException(
                    "dataset [" + name + "] references unknown data source [" + dataset.dataSource().getName() + "]"
                );
            }
            Map<String, Object> merged = mergeSettings(parent, dataset);
            Literal path = Literal.keyword(relation.source(), dataset.resource());
            children.add(new UnresolvedExternalRelation(relation.source(), path, merged));
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        // UnionAll inherits Fork's branch cap; wrap with a user-facing message instead of the internal
        // "FORK supports up to N branches" error from Fork's constructor.
        if (Fork.exceedsMaxBranches(children.size())) {
            throw new VerificationException(
                "FROM ["
                    + relation.indexPattern().indexPattern()
                    + "] matched "
                    + children.size()
                    + " datasets; current limit is "
                    + Fork.MAX_BRANCHES
                    + " per FROM. Narrow the pattern, exclude some datasets, or split into multiple queries."
            );
        }
        return new UnionAll(relation.source(), children, List.of());
    }

    /**
     * Returns {@code true} if any positive part of {@code patterns} could match a registered dataset
     * name. False positives are fine (slow path runs); false negatives would miss datasets, so this
     * must be at least as permissive as the full resolver.
     */
    private static boolean anyPatternCouldMatchDataset(List<String> patterns, Set<String> datasetNames) {
        if (datasetNames.isEmpty()) {
            return false;
        }
        for (String pattern : patterns) {
            if (pattern.isEmpty() || pattern.charAt(0) == '-') {
                continue;
            }
            // Date math (e.g. <logs-{now/d}>) needs the resolver's evaluator — fall through.
            if (pattern.charAt(0) == '<') {
                return true;
            }
            for (String dataset : datasetNames) {
                if (Regex.simpleMatch(pattern, dataset)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Dataset format settings at the top level; data-source auth/connection settings stored under
     * {@link ExternalSourceResolver#DATASOURCE_CONFIG_KEY} so they are kept separate from format
     * options. {@link ExternalSourceResolver#storageConfig} flattens the sub-map before passing
     * settings to a storage provider; {@link ExternalSourceResolver#planConfig} strips it before
     * embedding config in plan nodes (avoiding serialization of credential objects). A secret forwards
     * its raw value — an encrypted secret carries an {@code EncryptedData} the data-node decryption step
     * recognizes by type.
     */
    private static Map<String, Object> mergeSettings(DataSource parent, Dataset dataset) {
        Map<String, Object> merged = new HashMap<>();
        merged.putAll(dataset.settings());
        if (parent.settings().isEmpty() == false) {
            Map<String, Object> dsSettings = new HashMap<>();
            for (Map.Entry<String, DataSourceSetting> e : parent.settings()) {
                dsSettings.put(e.getKey(), e.getValue().secret() ? e.getValue().rawValue() : e.getValue().nonSecretValue());
            }
            merged.put(ExternalSourceResolver.DATASOURCE_CONFIG_KEY, dsSettings);
        }
        return merged;
    }
}
