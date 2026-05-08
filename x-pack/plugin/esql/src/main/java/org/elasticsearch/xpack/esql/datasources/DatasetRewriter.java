/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
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
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

    /** Built from {@link IndexResolver#DEFAULT_OPTIONS}; only delta is {@code resolveDatasets(true)}. */
    private static final IndicesOptions REWRITER_OPTIONS = IndicesOptions.builder(IndexResolver.DEFAULT_OPTIONS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(false).build())
        .build();

    private DatasetRewriter() {}

    /**
     * Walks {@code parsed} and rewrites every {@link UnresolvedRelation} whose pattern resolves to
     * dataset(s) into {@link UnresolvedExternalRelation} (single dataset) or {@link UnionAll} of
     * such (multi). All other relations are left untouched. Three short-circuits avoid resolver
     * cost on the common path: feature flag off, {@code projectMetadata == null}, or no datasets
     * registered.
     *
     * <p>Throws {@link VerificationException} for: heterogeneous FROM (datasets + non-datasets),
     * non-{@code STANDARD} {@link IndexMode} on a dataset, METADATA fields on a dataset, or
     * {@code UnionAll} branch-cap exceeded. Designed to run once on the parsed plan before
     * pre-analysis (so the analyzer sees a uniform {@code UnresolvedExternalRelation} tree
     * regardless of whether the user wrote {@code FROM <dataset>} or inline {@code EXTERNAL}).
     */
    public static LogicalPlan rewrite(LogicalPlan parsed, ProjectMetadata projectMetadata, IndexNameExpressionResolver iner) {
        if (DataSourceMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled() == false || projectMetadata == null) {
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
            r -> rewriteOne(r, projectMetadata, datasetMetadata, dataSourceMetadata, resolver)
        );
    }

    private static LogicalPlan rewriteOne(
        UnresolvedRelation relation,
        ProjectMetadata projectMetadata,
        DatasetMetadata datasets,
        DataSourceMetadata dataSources,
        IndexAbstractionResolver resolver
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
        var resolved = resolver.resolveIndexAbstractions(
            patterns,
            REWRITER_OPTIONS,
            projectMetadata,
            componentSelector -> indicesLookup.keySet(),
            (name, selector) -> true,
            true
        );

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
                datasetNames.add(name);
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
     * Parent settings overlaid by dataset settings. Secrets stay as
     * {@link org.elasticsearch.common.settings.SecureString}; callers materialize plaintext via
     * {@link Object#toString()} at use site. Use {@code Objects.toString(config.get(key), null)}
     * — direct {@code (String)} cast would CCE on non-String values.
     */
    private static Map<String, Object> mergeSettings(DataSource parent, Dataset dataset) {
        Map<String, Object> merged = new HashMap<>();
        for (Map.Entry<String, DataSourceSetting> e : parent.settings().entrySet()) {
            merged.put(e.getKey(), e.getValue().secret() ? e.getValue().secretValue() : e.getValue().nonSecretValue());
        }
        merged.putAll(dataset.settings());
        return merged;
    }
}
