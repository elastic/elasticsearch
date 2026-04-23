/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrites {@code FROM <dataset>} to the same {@link UnresolvedExternalRelation} the {@code EXTERNAL}
 * command produces, so both paths converge at the existing resolver + analyzer. Runs once on the
 * parsed plan before pre-analysis. Phase 1 rejects mixed indices-and-datasets patterns.
 */
public final class DatasetRewriter {

    private DatasetRewriter() {}

    public static LogicalPlan rewrite(LogicalPlan parsed, ProjectMetadata projectMetadata) {
        if (DataSourceMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled() == false) {
            return parsed;
        }
        DatasetMetadata datasetMetadata = DatasetMetadata.get(projectMetadata);
        if (datasetMetadata.datasets().isEmpty()) {
            return parsed;
        }
        DataSourceMetadata dataSourceMetadata = DataSourceMetadata.get(projectMetadata);
        return parsed.transformUp(UnresolvedRelation.class, r -> rewriteOne(r, datasetMetadata, dataSourceMetadata));
    }

    private static LogicalPlan rewriteOne(UnresolvedRelation relation, DatasetMetadata datasets, DataSourceMetadata dataSources) {
        List<String> names = splitPattern(relation.indexPattern().indexPattern());
        List<String> datasetNames = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();
        for (String name : names) {
            if (datasets.get(name) != null) {
                datasetNames.add(name);
            } else {
                indexNames.add(name);
            }
        }
        if (datasetNames.isEmpty()) {
            return relation;
        }
        if (indexNames.isEmpty() == false) {
            throw new VerificationException(
                "FROM mixing indices and datasets is not supported yet; requested mix: indices=" + indexNames + ", datasets=" + datasetNames
            );
        }
        List<LogicalPlan> children = new ArrayList<>(datasetNames.size());
        for (String name : datasetNames) {
            Dataset dataset = datasets.get(name);
            DataSource parent = dataSources.get(dataset.dataSource().getName());
            if (parent == null) {
                throw new VerificationException(
                    "dataset [" + name + "] references unknown data source [" + dataset.dataSource().getName() + "]"
                );
            }
            Map<String, Object> merged = mergeSettings(parent, dataset);
            Literal path = Literal.keyword(relation.source(), dataset.resource());
            children.add(new UnresolvedExternalRelation(relation.source(), path, toParams(relation.source(), merged)));
        }
        return children.size() == 1 ? children.get(0) : new UnionAll(relation.source(), children, List.of());
    }

    private static List<String> splitPattern(String pattern) {
        return Arrays.stream(pattern.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).toList();
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
