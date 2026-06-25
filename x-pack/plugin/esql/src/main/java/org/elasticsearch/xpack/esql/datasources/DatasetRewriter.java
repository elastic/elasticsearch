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
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.DatasetShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.IndexResolver;

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

    private static final Logger logger = LogManager.getLogger(DatasetRewriter.class);

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

        // (b) classify the raw (un-narrowed) patterns into dataset vs non-dataset under an open predicate.
        IndexAbstractionResolver resolver = new IndexAbstractionResolver(iner);
        Map<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        List<String> localNames = resolver.resolveIndexAbstractions(
            Arrays.asList(rawPatterns),
            RESOLVER_OPTIONS,
            projectMetadata,
            componentSelector -> indicesLookup.keySet(),
            (name, selector) -> true,
            true
        ).getLocalIndicesList();

        boolean hasNonDatasetTargets = false;
        Set<String> rawDatasetNames = new LinkedHashSet<>();
        for (String name : localNames) {
            IndexAbstraction abs = indicesLookup.get(name);
            if (abs == null) {
                continue; // synthesized name (date math) — neither; skipping doesn't suppress mixed-FROM rejection
            }
            if (abs.getType() == IndexAbstraction.Type.DATASET) {
                rawDatasetNames.add(name);
            } else {
                hasNonDatasetTargets = true;
            }
        }

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
        return new DatasetResolution(result, hasNonDatasetTargets, explicitUnauthorized);
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
     * <p>Throws {@link VerificationException} for: heterogeneous FROM (datasets + non-datasets), non-{@code STANDARD}
     * {@link IndexMode} on a dataset, or {@code UnionAll} branch-cap exceeded. Designed
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
            return rewriteOne(r, datasetMetadata, dataSourceMetadata, resolution, crossProjectEnabled);
        });
    }

    private static LogicalPlan rewriteOne(
        UnresolvedRelation relation,
        DatasetMetadata datasets,
        DataSourceMetadata dataSources,
        DatasetResolution resolution,
        boolean crossProjectEnabled
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
        if (resolution.hasNonDatasetTargets()) {
            // Counts only in the user-facing message (names may be unreadable to the caller); full names go to DEBUG for
            // operator triage. Rejection demanded by heterogeneous FROM (datasets + indices/aliases/data-streams).
            logger.debug(
                "DatasetRewriter rejecting mixed FROM: pattern=[{}] datasets={} (relation also targets non-dataset abstractions)",
                relation.indexPattern().indexPattern(),
                datasetNames
            );
            throw new VerificationException(
                "FROM mixing datasets and non-datasets is not supported; the pattern also matched non-dataset target(s) "
                    + "alongside "
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
            // Thread the user's METADATA clause through to the external leaf so
            // ResolveExternalRelations binds each requested name to an ExternalMetadataAttribute of
            // the type registered in MetadataAttribute.ATTRIBUTES_MAP. Every name in that map is
            // accepted on external datasets; values are framework-synthesized by the COMPOSED path.
            // The dataset name rides alongside so the per-file _index synthesizer can populate
            // _index with the user-facing identifier rather than the underlying resource path.
            children.add(new UnresolvedExternalRelation(relation.source(), path, merged, relation.metadataFields(), name));
        }
        // Friendly cap on the matched LOCAL datasets, checked before the CPS siblings are appended so the message
        // reports the real dataset count rather than the bookkeeping siblings. A sibling's branch contribution is
        // non-deterministic here: a wildcard-preserve relation or an exact-name DatasetShadowRelation becomes a branch
        // only if it resolves to a remote index (an unmatched shadow strips to nothing). Those resolved branches are
        // bounded separately by Fork's own MAX_BRANCHES cap post-resolution; counting them here would over-reject the
        // common case where they strip away.
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
        // Cross-project (CPS): a wildcard that matched a dataset locally may also match indices in linked projects.
        // Mirror ViewResolver — keep the original wildcard as a sibling UnresolvedRelation so the remote half resolves
        // at field-caps, instead of the dataset replacing the whole relation and silently dropping the remote indices.
        // Reached only when the wildcard matched no local non-dataset target (a local index/dataset mix is rejected
        // above), so the preserved relation contributes only the remote indices the wildcard pulls in.
        if (crossProjectEnabled) {
            List<String> remotePatterns = crossProjectPatternsToPreserve(patternsOf(relation));
            if (remotePatterns.isEmpty() == false) {
                children.add(
                    new UnresolvedRelation(
                        relation.source(),
                        new IndexPattern(relation.indexPattern().source(), String.join(",", remotePatterns)),
                        relation.frozen(),
                        relation.metadataFields(),
                        relation.indexMode(),
                        relation.unresolvedMessage()
                    )
                );
            }
            // CPS: an exact (non-wildcard) dataset name has no wildcard to re-emit above, so its remote half would never
            // reach field-caps. Emit a DatasetShadowRelation per exact authorized dataset name — the dataset analog of
            // ViewResolver's OPTIONAL-shadow branch — so the lenient linked pass federates a remote index of the same
            // name in, while a remote dataset/view of the same name fails (the detection rail). See
            // DatasetShadowRelation for the full lifecycle.
            for (DatasetShadowRelation shadow : crossProjectExactNameShadows(relation, datasetNames)) {
                children.add(shadow);
            }
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new UnionAll(relation.source(), children, List.of());
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
     * linked projects that a wildcard also matched. Positive wildcards are preserved — an exact dataset name is fully
     * handled by its external relation and needs no remote pass; exclusions ride along so they still apply to the
     * remote half. Returns empty when no positive wildcard is present (an exclusion-only relation has nothing to
     * match). Mirrors the wildcard pass-through in {@code ViewResolver.buildOrderedSubqueries}.
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
     * Builds a {@link DatasetShadowRelation} for each exact (non-wildcard, flat) dataset name in {@code datasetNames}
     * that the relation named with a concrete pattern, so the remote half of that name reaches the lenient linked pass.
     * Mirrors {@code ViewResolver}'s OPTIONAL-shadow branch: each shadow's pattern is the exact name followed by the
     * relation's trailing exclusions, so the remote resolution honors the same exclusions the local FROM did.
     * <p>
     * Only exact names produce shadows here — wildcards are already handled by {@link #crossProjectPatternsToPreserve}
     * (re-emitted as an {@link UnresolvedRelation}, which the strict main pass resolves). A remote-prefixed FROM never
     * reaches {@code rewriteOne} (see {@link #hasRemotePattern}), so every pattern here is flat.
     */
    static List<DatasetShadowRelation> crossProjectExactNameShadows(UnresolvedRelation relation, List<String> datasetNames) {
        List<String> patterns = patternsOf(relation);
        Set<String> datasetNameSet = new LinkedHashSet<>(datasetNames);
        List<DatasetShadowRelation> shadows = new ArrayList<>();
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
            List<String> shadowPattern = new ArrayList<>();
            shadowPattern.add(name);
            for (int p = i + 1; p < patterns.size(); p++) {
                String later = patterns.get(p);
                if (later.isEmpty() == false && later.charAt(0) == '-') {
                    shadowPattern.add(later);
                }
            }
            shadows.add(
                new DatasetShadowRelation(relation.source(), name, LinkedIndexPattern.Kind.OPTIONAL, String.join(",", shadowPattern))
            );
        }
        return shadows;
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
     * Per-relation result of {@link #resolve}: the authorized concrete dataset names the relation targets, whether it
     * also targets non-dataset abstractions (drives mixed-FROM rejection), and the explicitly-named datasets absent
     * from the authorized set (surfaced by {@link #rewriteOne} as {@code Unknown index}).
     */
    public record DatasetResolution(Set<String> authorizedDatasets, boolean hasNonDatasetTargets, Set<String> explicitUnauthorized) {}
}
