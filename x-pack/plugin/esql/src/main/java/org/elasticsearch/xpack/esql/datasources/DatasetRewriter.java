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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites {@code FROM <dataset>} (and patterns that match datasets) into the same
 * {@link UnresolvedExternalRelation} the {@code EXTERNAL} command produces, so both paths converge at
 * the existing resolver + analyzer. Runs once on the parsed plan before pre-analysis.
 *
 * <p>Pattern expansion (wildcards, exclusions, date math, hidden flag) is delegated to
 * {@link IndexAbstractionResolver} with {@code resolveDatasets(true)} — the same machinery
 * {@code FROM <index>} uses, so dataset names participate in the standard FROM syntax without a
 * parallel resolution path. Phase 1 rejects patterns that resolve to a mix of indices and datasets.
 */
public final class DatasetRewriter {

    private static final Logger logger = LogManager.getLogger(DatasetRewriter.class);

    /**
     * Permissive options for the rewriter's pattern-expansion step. We ask the resolver for
     * <em>everything that exists</em> matching the pattern (including datasets), then bucket by
     * abstraction type. Anything that doesn't resolve is left for the downstream analyzer to handle
     * with the request's actual {@link IndicesOptions} — surfacing the right error at the right layer
     * rather than competing with the analyzer.
     */
    private static final IndicesOptions REWRITER_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            IndicesOptions.WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .build()
        )
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
        // Fast path: the resolver expansion below is O(indices) regardless of dataset count, which
        // costs every query on a cluster with many indices. Skip it when no positive part of the pattern
        // can possibly match a registered dataset name. Conservative: false negatives only proceed
        // through the slow path, never miss a dataset.
        List<String> patterns = Arrays.asList(Strings.splitStringByCommaToArray(relation.indexPattern().indexPattern()));
        if (anyPatternCouldMatchDataset(patterns, datasets.datasets().keySet()) == false) {
            return relation;
        }

        Map<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        // Expand the pattern through the same machinery FROM <index> uses; the resolver handles
        // wildcards, exclusions, date math, and hidden flags identically for indices and datasets.
        var resolved = resolver.resolveIndexAbstractions(
            patterns,
            REWRITER_OPTIONS,
            projectMetadata,
            componentSelector -> indicesLookup.keySet(),
            (name, selector) -> true,
            true
        );

        List<String> datasetNames = new ArrayList<>();
        int nonDatasetCount = 0;
        for (String name : resolved.getLocalIndicesList()) {
            IndexAbstraction abs = indicesLookup.get(name);
            if (abs == null) {
                // Race window: the resolver returned a name that is no longer in the lookup. Cluster
                // state can change between the resolver call and the lookup. Treat as transient miss
                // and skip; downstream analyzer will produce the right error if the name is required.
                logger.debug("DatasetRewriter: resolved name [{}] not found in indices lookup; skipping", name);
                continue;
            }
            if (abs.getType() == IndexAbstraction.Type.DATASET) {
                datasetNames.add(name);
            } else {
                nonDatasetCount++;
            }
        }

        if (datasetNames.isEmpty()) {
            // No dataset references in this pattern; let the downstream analyzer resolve as usual.
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
        if (nonDatasetCount > 0) {
            // Surface counts only — listing matched names would exfiltrate index/alias/data-stream
            // names the caller may not have read access to.
            throw new VerificationException(
                "FROM mixing datasets and non-datasets is not supported; requested mix: "
                    + nonDatasetCount
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
            // data source, so a dataset with a missing parent should only happen if that invariant breaks
            // (e.g. a broken cluster-state restore). Throw with explicit context — assert was previously
            // used here, but asserts are off in production and the next line would NPE without context.
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
        // Wrap the platform's UnionAll/Fork branch cap with a user-facing message. Without this, the
        // caller hits Fork's constructor with "FORK supports up to 8 branches" — but FORK is an
        // internal plan-node name; the user's query said FROM <pattern>. Tracked as the
        // fix at esql-planning#614 (raise the cap or coalesce siblings); meanwhile fail with framing
        // the user can act on.
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
     * Conservative fast-path predicate: returns {@code true} if any positive part of {@code patterns}
     * could match a registered dataset name (literal or wildcard). False positives lead to the slow
     * path running uselessly; false negatives would miss datasets, so wildcard semantics here must be
     * at least as permissive as the full resolver's.
     */
    private static boolean anyPatternCouldMatchDataset(List<String> patterns, Set<String> datasetNames) {
        if (datasetNames.isEmpty()) {
            return false;
        }
        for (String pattern : patterns) {
            if (pattern.isEmpty() || pattern.charAt(0) == '-') {
                // Exclusion, or empty token from a malformed pattern; can't pull a dataset into the result.
                continue;
            }
            // Strip a cluster qualifier (`cluster:name`) — datasets are local-only today, so a qualified
            // pattern can't match a local dataset name. Conservative: still check the local part.
            int colon = pattern.indexOf(':');
            String local = colon >= 0 ? pattern.substring(colon + 1) : pattern;
            for (String dataset : datasetNames) {
                if (Regex.simpleMatch(local, dataset)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Parent data source settings overlaid by the dataset's settings. Returns plain
     * {@code Map<String, Object>} — no {@code Literal} wrapping.
     *
     * <p><b>Secret values are kept as {@link org.elasticsearch.common.settings.SecureString} in the
     * map.</b> Consumers (data-source plugin code that reads credentials) should call
     * {@link Object#toString()} at the point of use to materialize the plaintext, and ideally
     * {@code close()} the SecureString afterward to clear the heap-resident chars. Calling
     * {@code (String) config.get("secret_key")} will fail with a ClassCastException — use
     * {@code Objects.toString(config.get("secret_key"), null)} or equivalent.
     */
    private static Map<String, Object> mergeSettings(DataSource parent, Dataset dataset) {
        Map<String, Object> merged = new HashMap<>();
        for (Map.Entry<String, DataSourceSetting> e : parent.settings().entrySet()) {
            // For secret values, keep the SecureString rather than calling .toString() — that produces
            // a heap-resident immutable String for the plaintext that lives until GC. SecureString
            // can be close()'d by the consumer to zero its backing char[] right after auth.
            merged.put(e.getKey(), e.getValue().secret() ? e.getValue().secretValue() : e.getValue().nonSecretValue());
        }
        merged.putAll(dataset.settings());
        return merged;
    }
}
