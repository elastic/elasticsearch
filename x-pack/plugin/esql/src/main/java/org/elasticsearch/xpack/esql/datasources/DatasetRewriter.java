/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.schema.AbstractionResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites {@code FROM <dataset>} into the same {@link UnresolvedExternalRelation} the
 * {@code EXTERNAL} command produces, so both paths converge at the existing resolver + analyzer.
 * Runs once on the parsed plan before pre-analysis.
 *
 * <p>Pattern expansion (wildcards, exclusions, date math, hidden flag) is <b>not</b> done here. It happens in the
 * authorization engine: {@link DatasetResolver} dispatches each relation's raw FROM patterns to
 * {@link org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction}, whose body calls {@link #resolve} to expand
 * the patterns against the caller's authorized abstractions (wildcard expansion lands where authorization lives, not
 * client-side). {@link #resolve} returns, per relation, the authorized concrete dataset names plus whether the
 * relation also targets non-dataset abstractions. {@link #rewrite}/{@link #rewriteOne} then consume that
 * {@link DatasetResolution} to build the plan — they no longer resolve, expand, or gate on authorization.
 */
public final class DatasetRewriter {

    /**
     * {@link IndexResolver#DEFAULT_OPTIONS} (which carries {@code ALLOW_UNAVAILABLE_TARGETS}) plus
     * {@code resolveDatasets(true)}. Shared with
     * {@link org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction.Request}, so the security filter silently
     * narrows an unauthorized concrete dataset name to nothing rather than throwing a {@code 403} — a {@code 403} on an
     * explicit name would be an existence oracle ("this dataset exists but you can't read it"). Existence-hiding is
     * completed in {@link #rewriteOne}, which surfaces an explicitly-named-but-unauthorized dataset as the same
     * {@code Unknown index} ({@code 400}) a missing index gives — see {@link #resolve}.
     */
    public static final IndicesOptions RESOLVER_OPTIONS = IndicesOptions.builder(IndexResolver.DEFAULT_OPTIONS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(false).build())
        .build();

    private DatasetRewriter() {}

    /**
     * Per-relation engine-side resolution, run from the {@code EsqlResolveDatasetAction} body. Returns the authorized
     * dataset names, whether the relation also targets non-dataset abstractions (drives mixed-FROM rejection), and the
     * explicitly-named-but-unauthorized datasets — which {@link #rewriteOne} surfaces as {@code Unknown index} (400),
     * the same error a missing index gives, so an unauthorized dataset can't be told apart from a missing name.
     */
    public static DatasetResolution resolve(
        String[] authorizedIndices,
        String[] rawPatterns,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver iner
    ) {
        // (a) authorized datasets: request.indices(), which the filter already narrowed to the authorized subset on a
        // secured cluster (and equals rawPatterns without security). Empty short-circuits, else an empty list would
        // normalize to "_all" and re-expand to every dataset.
        Set<String> authorizedDatasets = authorizedIndices.length == 0
            ? new LinkedHashSet<>()
            : new LinkedHashSet<>(iner.datasets(projectMetadata, RESOLVER_OPTIONS, indicesRequestOf(authorizedIndices)));

        // (b) classify the raw (un-narrowed) patterns into dataset vs non-dataset under an open predicate, through the
        // kind-blind front (stage ①). The front wraps the same IndexAbstractionResolver call and classifies each
        // concrete local name by IndexAbstraction.Type; a name absent from the lookup (date math) is of no kind and is
        // neither counted as a dataset nor as a non-dataset target — identical to the inline classification it replaces.
        Map<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        AbstractionResolver.Resolution resolution = new AbstractionResolver(iner).resolve(
            Arrays.asList(rawPatterns),
            RESOLVER_OPTIONS,
            projectMetadata,
            componentSelector -> indicesLookup.keySet(),
            (name, selector) -> true
        );
        Set<String> rawDatasetNames = new LinkedHashSet<>(resolution.namesOfKind(IndexAbstraction.Type.DATASET));
        // Concrete non-dataset names (indices, aliases, data streams) resolved from the same pattern. These build the
        // heterogeneous-FROM index branch in rewriteOne — a mixed FROM idx, ds unions the dataset leaves with an index
        // relation over these names rather than being rejected (#151977).
        Set<String> nonDatasetNames = new LinkedHashSet<>(resolution.namesNotOfKind(IndexAbstraction.Type.DATASET));

        // Explicit (non-wildcard) dataset names absent from the authorized set — rewriteOne rejects these as Unknown
        // index rather than silently dropping them from a multi-target FROM.
        Set<String> explicitUnauthorized = new LinkedHashSet<>();
        for (String pattern : rawPatterns) {
            if (pattern.isEmpty() || pattern.charAt(0) == '-' || Regex.isSimpleMatchPattern(pattern)) {
                continue;
            }
            String name = IndexNameExpressionResolver.resolveDateMathExpression(pattern);
            if (rawDatasetNames.contains(name) && authorizedDatasets.contains(name) == false) {
                explicitUnauthorized.add(name);
            }
        }

        Set<String> result = new LinkedHashSet<>(rawDatasetNames);
        result.retainAll(authorizedDatasets);
        return new DatasetResolution(result, nonDatasetNames, explicitUnauthorized);
    }

    /** Minimal {@link IndicesRequest} carrier so {@link IndexNameExpressionResolver#datasets} can read the names. */
    private static IndicesRequest indicesRequestOf(String[] indices) {
        return new IndicesRequest() {
            @Override
            public String[] indices() {
                return indices;
            }

            @Override
            public IndicesOptions indicesOptions() {
                return RESOLVER_OPTIONS;
            }
        };
    }

    /**
     * Convenience entry for an unsecured context (security disabled, or tests): resolves every dataset-candidate
     * relation with the full authorized set and {@link #rewrite}s — the in-process equivalent of
     * {@link DatasetResolver}'s dispatch, minus the {@code EsqlResolveDatasetAction} round-trip. {@code null} or
     * dataset-free project is a no-op.
     */
    public static LogicalPlan rewriteUnsecured(LogicalPlan parsed, ProjectMetadata projectMetadata, IndexNameExpressionResolver iner) {
        if (projectMetadata == null) {
            return parsed;
        }
        Set<String> datasetNames = DatasetMetadata.get(projectMetadata).datasets().keySet();
        if (datasetNames.isEmpty()) {
            return parsed;
        }
        Map<UnresolvedRelation, DatasetResolution> resolutions = new IdentityHashMap<>();
        parsed.forEachUp(UnresolvedRelation.class, r -> {
            if (resolutions.containsKey(r)) {
                return;
            }
            List<String> patterns = patternsOf(r);
            if (hasRemotePattern(patterns) || anyPatternCouldMatchDataset(patterns, datasetNames) == false) {
                return;
            }
            // Unsecured: the (un-narrowed) raw patterns are the authorized indices — every registered dataset matched
            // by the pattern is authorized, so resolve() returns it.
            String[] raw = patterns.toArray(String[]::new);
            resolutions.put(r, resolve(raw, raw, projectMetadata, iner));
        });
        // Unsecured/test path runs without CPS (single local project): never preserve a wildcard for remote resolution.
        return rewrite(parsed, projectMetadata, resolutions, false);
    }

    static boolean hasRemotePattern(List<String> patterns) {
        for (String pattern : patterns) {
            if (RemoteClusterAware.isRemoteIndexName(pattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Walks {@code parsed} and rewrites every {@link UnresolvedRelation} that resolved to authorized dataset(s) into
     * {@link UnresolvedExternalRelation} (single dataset) or {@link UnionAll} of such (multi), using the per-relation
     * {@link DatasetResolution} computed engine-side by {@link #resolve}. All other relations are left untouched. The
     * {@code projectMetadata == null} / no-datasets-registered short-circuits avoid touching the common path.
     *
     * <p>A heterogeneous FROM (datasets + non-datasets) is <b>not</b> rejected — the dataset leaves and an index
     * relation over the non-dataset names union into one {@link UnionAll} (#151977). Throws {@link VerificationException}
     * for: non-{@code STANDARD} {@link IndexMode} on a dataset, or {@code UnionAll} branch-cap exceeded. Designed
     * to run once on the parsed plan before pre-analysis (so the analyzer sees a uniform
     * {@code UnresolvedExternalRelation} tree regardless of whether the user wrote {@code FROM <dataset>} or inline
     * {@code EXTERNAL}).
     *
     * @param resolutions per-relation resolution keyed by relation identity (see {@link DatasetResolver}). A relation
     *                    absent from the map (e.g. remote-prefixed, or no pattern could match a dataset name) is left
     *                    unchanged.
     * @param crossProjectEnabled whether cross-project search (CPS) is active; when {@code true}, a wildcard that
     *                    matched a dataset is kept alongside the dataset so the remote (linked-project) half still
     *                    resolves — see {@link #rewriteOne}.
     */
    public static LogicalPlan rewrite(
        LogicalPlan parsed,
        ProjectMetadata projectMetadata,
        Map<UnresolvedRelation, DatasetResolution> resolutions,
        boolean crossProjectEnabled
    ) {
        return rewrite(parsed, projectMetadata, resolutions, crossProjectEnabled, null);
    }

    /**
     * As {@link #rewrite(LogicalPlan, ProjectMetadata, Map, boolean)}, but consuming dataset configs already resolved
     * through the schema umbrella. When {@code preResolvedConfigs} is non-null it is the authoritative source of each
     * authorized dataset's merged external-source config (keyed by dataset name); {@link #rewriteOne} reads it instead
     * of recomputing the merge inline. The map and the inline {@link #mergeSettings} compute byte-identical config, so
     * the rewrite is behaviour-identical either way — the umbrella path just moves the config production behind the
     * singular {@code resolveSchema} dispatch. A {@code null} map keeps the legacy inline computation (the unsecured /
     * test entries that have no umbrella to call).
     */
    public static LogicalPlan rewrite(
        LogicalPlan parsed,
        ProjectMetadata projectMetadata,
        Map<UnresolvedRelation, DatasetResolution> resolutions,
        boolean crossProjectEnabled,
        Map<String, Map<String, Object>> preResolvedConfigs
    ) {
        if (projectMetadata == null) {
            return parsed;
        }
        DatasetMetadata datasetMetadata = DatasetMetadata.get(projectMetadata);
        if (datasetMetadata.datasets().isEmpty()) {
            return parsed;
        }
        DataSourceMetadata dataSourceMetadata = DataSourceMetadata.get(projectMetadata);
        return parsed.transformUp(UnresolvedRelation.class, r -> {
            DatasetResolution resolution = resolutions.get(r);
            if (resolution == null) {
                return r;
            }
            return rewriteOne(r, datasetMetadata, dataSourceMetadata, resolution, crossProjectEnabled, preResolvedConfigs);
        });
    }

    /**
     * The set of authorized concrete dataset names across all relation resolutions — the names whose configs the
     * umbrella resolves up front so {@link #rewrite} can consume them. Mirrors the names {@link #rewriteOne} would
     * otherwise resolve inline, so resolving exactly these is behaviour-identical.
     */
    public static Set<String> authorizedDatasetNames(Map<UnresolvedRelation, DatasetResolution> resolutions) {
        Set<String> names = new LinkedHashSet<>();
        for (DatasetResolution resolution : resolutions.values()) {
            names.addAll(resolution.authorizedDatasets());
        }
        return names;
    }

    private static LogicalPlan rewriteOne(
        UnresolvedRelation relation,
        DatasetMetadata datasets,
        DataSourceMetadata dataSources,
        DatasetResolution resolution,
        boolean crossProjectEnabled,
        Map<String, Map<String, Object>> preResolvedConfigs
    ) {
        if (resolution.explicitUnauthorized().isEmpty() == false) {
            // An explicitly-named dataset the caller can't read — same error (and 400) a missing index gives, so an
            // unauthorized dataset is indistinguishable from a nonexistent name. No existence oracle.
            throw new VerificationException("Unknown index [" + resolution.explicitUnauthorized().iterator().next() + "]");
        }

        List<String> datasetNames = new ArrayList<>(resolution.authorizedDatasets());

        if (datasetNames.isEmpty()) {
            // Nothing authorized (or matched) here: the relation flows through to index resolution unchanged. Note this
            // path is reached even when the relation has non-dataset targets — an ordinary FROM <index> looks exactly
            // like this and must not be rejected as a "mix".
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
        // One rail for every FROM shape — dataset-only and heterogeneous (index + dataset). A mixed FROM idx, ds unions
        // the dataset leaves with an index relation over the concrete non-dataset names (#151977); the non-remotable-
        // abstraction CPS rule (a remote view/dataset fails; a remote index of the same name reads both) holds
        // uniformly because the cross-project siblings below are appended regardless of whether the FROM also names
        // local indices. Keeping the two shapes on one path is what stops them drifting.
        List<LogicalPlan> children = new ArrayList<>(datasetNames.size());
        for (String name : datasetNames) {
            Dataset dataset = datasets.get(name);
            // Config comes from the umbrella's singular resolveSchema dispatch when present (the live production path),
            // else from the inline merge (the unsecured / test entries). Both compute the same merged config, so the
            // resulting external relation is identical. The null-parent broken-invariant guard lives in datasetConfig
            // for the umbrella path and below for the inline path.
            Map<String, Object> merged;
            if (preResolvedConfigs != null) {
                merged = preResolvedConfigs.get(name);
                if (merged == null) {
                    throw new IllegalStateException("no resolved config for authorized dataset [" + name + "]");
                }
            } else {
                DataSource parent = dataSources.get(dataset.dataSource().getName());
                // DataSourceService.deleteDataSources rejects (409) on orphans, so a null parent here
                // means a broken-invariant state (e.g. corrupt cluster-state restore) — throw with context.
                if (parent == null) {
                    throw new IllegalStateException(
                        "dataset [" + name + "] references unknown data source [" + dataset.dataSource().getName() + "]"
                    );
                }
                merged = mergeSettings(parent, dataset);
            }
            Literal path = Literal.keyword(relation.source(), dataset.resource());
            // Thread the user's METADATA clause through to the external leaf so
            // ResolveExternalRelations binds each requested name to an ExternalMetadataAttribute of
            // the type registered in MetadataAttribute.ATTRIBUTES_MAP. Every name in that map is
            // accepted on external datasets; values are framework-synthesized by the COMPOSED path.
            // The dataset name rides alongside so the per-file _index synthesizer can populate
            // _index with the user-facing identifier rather than the underlying resource path.
            UnresolvedExternalRelation external = new UnresolvedExternalRelation(
                relation.source(),
                path,
                merged,
                relation.metadataFields(),
                name
            );
            // Wrap each dataset's relation in a first-class Dataset node (LOCAL). The wrapper is transparent to
            // resolution — PreAnalyzer's UnresolvedExternalRelation path collection and the ResolveExternalRelations
            // rule both descend into the child, resolving it into the identical ExternalRelation produced today — and the
            // Mapper lowers the LOCAL Dataset by mapping that resolved child, so the external read is byte-identical. The
            // distinct node is what lets a REMOTE / MATERIALIZED dataset be lowered opaquely, off the inline-EXTERNAL path.
            children.add(new org.elasticsearch.xpack.esql.plan.logical.Dataset(relation.source(), name, external));
        }
        // Index branch: the concrete local non-dataset names plus, under cross-project, any preserved positive
        // wildcards — joined into one UnresolvedRelation so the resolver dedups a local index matched by both a
        // concrete name and a wildcard (no double read) and the wildcard's remote half reaches field-caps (closing
        // #151977's dropped-remote-wildcard gap). METADATA fields ride along so _index/_id resolve on the index rows.
        List<String> indexBranch = new ArrayList<>(resolution.nonDatasetNames());
        if (crossProjectEnabled) {
            indexBranch.addAll(crossProjectPatternsToPreserve(patternsOf(relation)));
        }
        if (indexBranch.isEmpty() == false) {
            children.add(
                new UnresolvedRelation(
                    relation.source(),
                    new IndexPattern(relation.indexPattern().source(), String.join(",", indexBranch)),
                    relation.frozen(),
                    relation.metadataFields(),
                    relation.indexMode(),
                    relation.unresolvedMessage()
                )
            );
        }

        // Cap the real-read branches (datasets + the index branch) here, BEFORE the speculative linked siblings. A
        // linked relation strips when its name has no remote namesake (PruneEmptyUnionAllBranch), so it must not
        // consume the rewrite-time budget; a matched one is a real read bounded post-analysis by Fork.checkBranchCount.
        // UnionAll inherits Fork's branch cap; wrap with a user-facing message instead of the internal Fork error.
        if (Fork.exceedsMaxBranches(children.size())) {
            throw new VerificationException(
                "FROM ["
                    + relation.indexPattern().indexPattern()
                    + "] resolved to "
                    + children.size()
                    + " branches, exceeding the current limit of "
                    + Fork.MAX_BRANCHES
                    + " per FROM. Narrow the pattern, exclude some datasets, or split into multiple queries."
            );
        }

        // CPS: an exact (non-wildcard) dataset name has no wildcard to re-emit, so its remote half rides a linked
        // relation — the same views-style rail ViewResolver uses (UnresolvedRelation carrying a LinkedIndexPattern,
        // collected kind-blind by PreAnalyzer and resolved by Analyzer.ResolveLinkedRelations). A remote index of the
        // same name federates in; a remote dataset/view of the same name fails on the detection rail. This stays inert
        // until datasets exist: datasetNames is non-empty only once datasets are registered.
        if (crossProjectEnabled) {
            children.addAll(crossProjectExactNameLinkedRelations(relation, datasetNames));
        }

        if (children.size() == 1) {
            return children.get(0);
        }
        return new UnionAll(relation.source(), children, List.of());
    }

    /**
     * Builds a linked {@link UnresolvedRelation} for each exact (non-wildcard, flat) dataset name the relation named,
     * so the remote half of that name reaches the lenient linked pass. Mirrors {@code ViewResolver}'s OPTIONAL-linked
     * branch: each linked relation carries a {@link LinkedIndexPattern} of kind {@code OPTIONAL} whose pattern is the
     * exact name followed by the relation's trailing exclusions, so remote resolution honors the same exclusions the
     * local FROM did. {@link org.elasticsearch.xpack.esql.analysis.PreAnalyzer} collects these kind-blind and
     * {@code Analyzer.ResolveLinkedRelations} resolves them against the linked projects; an unmatched one is pruned by
     * {@code PruneEmptyUnionAllBranch}.
     * <p>
     * Only exact names produce linked relations — wildcards are already handled by {@link #crossProjectPatternsToPreserve}
     * (re-emitted into the index branch, which the strict main pass resolves). A remote-prefixed FROM never reaches
     * {@code rewriteOne} (see {@link #hasRemotePattern}), so every pattern here is flat.
     */
    static List<UnresolvedRelation> crossProjectExactNameLinkedRelations(UnresolvedRelation relation, List<String> datasetNames) {
        List<String> patterns = patternsOf(relation);
        Set<String> datasetNameSet = new LinkedHashSet<>(datasetNames);
        List<UnresolvedRelation> linked = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < patterns.size(); i++) {
            String pattern = patterns.get(i);
            if (pattern.isEmpty() || pattern.charAt(0) == '-' || Regex.isSimpleMatchPattern(pattern)) {
                continue;
            }
            // Resolve date-math so a literal-named dataset with a date suffix matches its authorized name.
            String name = IndexNameExpressionResolver.resolveDateMathExpression(pattern);
            if (datasetNameSet.contains(name) == false || seen.add(name) == false) {
                continue;
            }
            // Exclusions are positional (ES applies them left-to-right): only those appearing AFTER this name narrow it.
            // Mirrors ViewResolver.collectExclusionsAfterPosition.
            List<String> linkedPattern = new ArrayList<>();
            linkedPattern.add(name);
            for (int p = i + 1; p < patterns.size(); p++) {
                String later = patterns.get(p);
                if (later.isEmpty() == false && later.charAt(0) == '-') {
                    linkedPattern.add(later);
                }
            }
            IndexPattern indexPattern = new IndexPattern(relation.source(), String.join(",", linkedPattern));
            linked.add(
                new UnresolvedRelation(
                    relation.source(),
                    indexPattern,
                    relation.frozen(),
                    relation.metadataFields(),
                    IndexMode.STANDARD,
                    relation.unresolvedMessage(),
                    relation.telemetryLabel(),
                    new LinkedIndexPattern(LinkedIndexPattern.Kind.OPTIONAL, indexPattern)
                )
            );
        }
        return linked;
    }

    /**
     * Returns {@code true} if any positive part of {@code patterns} could match a registered dataset
     * name. False positives are fine (slow path runs); false negatives would miss datasets, so this
     * must be at least as permissive as the full resolver.
     */
    static boolean anyPatternCouldMatchDataset(List<String> patterns, Set<String> datasetNames) {
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

    /** Splits a relation's FROM pattern string into its comma-separated parts. */
    static List<String> patternsOf(UnresolvedRelation relation) {
        return Arrays.asList(Strings.splitStringByCommaToArray(relation.indexPattern().indexPattern()));
    }

    /**
     * The patterns to re-emit as an {@link UnresolvedRelation} so cross-project (CPS) resolution can reach indices in
     * linked projects that a wildcard also matched. Positive wildcards are preserved — an exact dataset name reaches
     * its remote half through a linked relation instead (see {@link #crossProjectExactNameLinkedRelations}), not this
     * pass; exclusions ride along so they still apply to the remote half. Returns empty when no positive wildcard is
     * present (an exclusion-only relation has nothing to match). Mirrors the wildcard pass-through in
     * {@code ViewResolver.buildOrderedSubqueries}.
     */
    static List<String> crossProjectPatternsToPreserve(List<String> patterns) {
        List<String> preserved = new ArrayList<>();
        boolean hasPositiveWildcard = false;
        for (String pattern : patterns) {
            if (pattern.isEmpty()) {
                continue;
            }
            if (pattern.charAt(0) == '-') {
                preserved.add(pattern);
            } else if (Regex.isSimpleMatchPattern(pattern)) {
                preserved.add(pattern);
                hasPositiveWildcard = true;
            }
        }
        return hasPositiveWildcard ? preserved : List.of();
    }

    /**
     * The merged external-source config for a single dataset name — the dataset's own settings plus its parent
     * data source's settings (secrets included). This is what the schema resolver hands back so the coordinator can
     * build the external relation, decoupled from the full {@link #rewrite} plan walk.
     */
    public static Map<String, Object> datasetConfig(ProjectMetadata projectMetadata, String name) {
        Dataset dataset = DatasetMetadata.get(projectMetadata).get(name);
        DataSource parent = DataSourceMetadata.get(projectMetadata).get(dataset.dataSource().getName());
        if (parent == null) {
            throw new IllegalStateException(
                "dataset [" + name + "] references unknown data source [" + dataset.dataSource().getName() + "]"
            );
        }
        return mergeSettings(parent, dataset);
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

    /**
     * Per-relation result of {@link #resolve}: the authorized concrete dataset names the relation targets, the concrete
     * non-dataset names resolved from the same pattern (drives heterogeneous-FROM {@link UnionAll} building), and the
     * explicitly-named datasets absent from the authorized set (surfaced by {@link #rewriteOne} as {@code Unknown index}).
     */
    public record DatasetResolution(Set<String> authorizedDatasets, Set<String> nonDatasetNames, Set<String> explicitUnauthorized) {}
}
