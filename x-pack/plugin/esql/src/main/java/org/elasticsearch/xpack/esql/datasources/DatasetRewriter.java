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

    /**
     * Built from {@link IndexResolver#DEFAULT_OPTIONS} so the rewriter sees the same set of
     * abstractions the user-side FROM path would. The only intentional delta is
     * {@code resolveDatasets(true)} — without it the resolver would skip datasets and the rewriter
     * would have nothing to rewrite. {@code resolveViews(false)} makes the view rewriter the sole
     * owner of view abstractions.
     */
    private static final IndicesOptions REWRITER_OPTIONS = IndicesOptions.builder(IndexResolver.DEFAULT_OPTIONS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(false).build())
        .build();

    private DatasetRewriter() {}

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

        // Datasets are local-only: skip dataset rewriting when any pattern is cluster-prefixed so the
        // original FROM (with all its parts intact) flows through to regular CCS resolution. Without
        // this guard, a coincidental local-dataset name match on the local part of a remote-prefixed
        // pattern would silently rewrite the query into a dataset query and drop the cluster prefix.
        for (String pattern : patterns) {
            if (RemoteClusterAware.isRemoteIndexName(pattern)) {
                return relation;
            }
        }

        // Fast path: skip the O(indices) resolver expansion when no positive pattern can match any
        // registered dataset name. Predicate is conservative — false positives just run the slow path,
        // false negatives would miss datasets so the predicate must be at least as permissive as the
        // resolver itself.
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
                // The resolver can synthesize a concrete name that doesn't exist as an abstraction —
                // notably date-math expansion under ALLOW_UNAVAILABLE_TARGETS, which is the user-side
                // default we mirror. A synthesized name is by definition neither a dataset nor a
                // real non-dataset, so skipping it is correct: it doesn't count toward
                // nonDatasetNames and can't accidentally suppress the mixed-FROM rejection.
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
        if (nonDatasetNames.isEmpty() == false) {
            // User-facing message uses counts only — listing matched names would exfiltrate names
            // the caller may not have read access to (resolver runs unauthz here; per-index authz
            // is applied later at field-caps). For operator triage, the full names are emitted at
            // DEBUG level only — never user-visible. This rejection is removed once heterogeneous
            // FROM is supported — the query becomes a UnionAll and field-caps applies authz on the
            // index side naturally.
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
            // DataSourceService.deleteDataSources rejects (409) when any dataset still references the
            // data source. A null parent therefore indicates a broken-invariant state (e.g. corrupt
            // cluster-state restore) — throw with context rather than rely on the assert one line below.
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
        // The user typed FROM <pattern>; the underlying UnionAll inherits Fork's branch cap. Wrap with
        // a message that names the user's pattern and the cap so the failure is actionable, rather than
        // hitting the internal "FORK supports up to N branches" message from Fork's constructor.
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
     * name (literal or wildcard). False positives lead to the slow path running uselessly; false
     * negatives would miss datasets, so wildcard semantics here must be at least as permissive as the
     * full resolver's.
     */
    private static boolean anyPatternCouldMatchDataset(List<String> patterns, Set<String> datasetNames) {
        if (datasetNames.isEmpty()) {
            return false;
        }
        for (String pattern : patterns) {
            if (pattern.isEmpty() || pattern.charAt(0) == '-') {
                continue;
            }
            // Date-math patterns (e.g. <logs-{now/d}>) resolve to a name only after the resolver
            // expands the date expression. We can't replicate that here without re-implementing
            // date-math evaluation, so fall through to the slow path conservatively.
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
     * Parent data source settings overlaid by the dataset's settings, returned as a plain
     * {@code Map<String, Object>}. Secret values are stored as
     * {@link org.elasticsearch.common.settings.SecureString} so the plan tree never stringifies them;
     * callers materialize plaintext via {@link Object#toString()} at the point of use. Bounding the
     * resulting plaintext lifetime is the consumer's responsibility — the {@code String} allocated
     * by {@code .toString()} is GC-bound, not closeable. Use
     * {@code Objects.toString(config.get(key), null)} rather than {@code (String)} which would CCE.
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
