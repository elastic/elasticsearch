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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        Map<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
        // Expand the pattern through the same machinery FROM <index> uses; the resolver handles
        // wildcards, exclusions, date math, and hidden flags identically for indices and datasets.
        List<String> patterns = List.of(Strings.splitStringByCommaToArray(relation.indexPattern().indexPattern()));
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
            if (abs != null && abs.getType() == IndexAbstraction.Type.DATASET) {
                datasetNames.add(name);
            } else if (abs != null) {
                nonDatasetNames.add(name);
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
        if (nonDatasetNames.isEmpty() == false) {
            throw new VerificationException(
                "FROM mixing indices and datasets is not supported; requested mix: indices="
                    + nonDatasetNames
                    + ", datasets="
                    + datasetNames
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
            children.add(new UnresolvedExternalRelation(relation.source(), path, toParams(relation.source(), merged)));
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        // Wrap the platform's UnionAll/Fork branch cap with a user-facing message. Without this, the
        // caller hits Fork's constructor with "FORK supports up to 8 branches" — but FORK is an
        // internal plan-node name; the user's query said FROM <pattern>. Tracked as the long-term
        // fix at esql-planning#614 (raise the cap or coalesce siblings); meanwhile fail with framing
        // the user can act on.
        if (children.size() > Fork.MAX_BRANCHES) {
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

    /** Parent data source settings (secrets unwrapped to plaintext) overlaid by the dataset's settings. */
    private static Map<String, Object> mergeSettings(DataSource parent, Dataset dataset) {
        Map<String, Object> merged = new HashMap<>();
        for (Map.Entry<String, DataSourceSetting> e : parent.settings().entrySet()) {
            merged.put(e.getKey(), e.getValue().secret() ? e.getValue().secretValue().toString() : e.getValue().nonSecretValue());
        }
        merged.putAll(dataset.settings());
        return merged;
    }

    private static Map<String, Expression> toParams(Source source, Map<String, Object> merged) {
        Map<String, Expression> params = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : merged.entrySet()) {
            params.put(e.getKey(), Literal.keyword(source, String.valueOf(e.getValue())));
        }
        return params;
    }
}
