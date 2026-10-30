/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.rank.vectors.eval.RankVectorsEvalResponse;
import org.elasticsearch.xpack.rank.vectors.eval.RestRankVectorsEvalAction;
import org.elasticsearch.xpack.rank.vectors.eval.TransportRankVectorsEvalAction;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;
import org.elasticsearch.xpack.rank.vectors.query.ExactRankVectorsQueryBuilder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsCandidateQueryBuilder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsQueryBuilder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsRetrieverBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.index.mapper.FieldMapper.notInMultiFields;
import static org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper.CONTENT_TYPE;

public class RankVectorsPlugin extends Plugin implements MapperPlugin, SearchPlugin, ActionPlugin {
    public static final LicensedFeature.Momentary RANK_VECTORS_FEATURE = LicensedFeature.momentary(
        null,
        "rank-vectors",
        License.OperationMode.ENTERPRISE
    );
    public static final ActionType<RankVectorsEvalResponse> RANK_VECTORS_EVAL_ACTION = new ActionType<>(
        "indices:data/read/rank_vectors_eval"
    );
    public static final Setting<Boolean> RANK_VECTORS_EVAL_ENABLED = Setting.boolSetting(
        "search.rank_vectors_eval.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );
    public static final Setting<Boolean> INDEX_RANK_VECTORS_EVAL_ENABLED = Setting.boolSetting(
        "index.rank_vectors_eval.enabled",
        true,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.ServerlessPublic
    );

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(CONTENT_TYPE, new FieldMapper.TypeParser((n, c) -> {
            if (RANK_VECTORS_FEATURE.check(getLicenseState()) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            return new RankVectorsFieldMapper.Builder(
                n,
                c.indexVersionCreated(),
                c.getIndexSettings().getMode(),
                getLicenseState(),
                INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(c.getIndexSettings().getSettings()),
                DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.get(c.getIndexSettings().getSettings()),
                c.getVectorsFormatProviders()
            );
        }, notInMultiFields(CONTENT_TYPE)));
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(RankVectorsQueryBuilder.NAME, RankVectorsQueryBuilder::new, RankVectorsQueryBuilder::fromXContent),
            new QuerySpec<>(ExactRankVectorsQueryBuilder.NAME, ExactRankVectorsQueryBuilder::new, parser -> {
                throw new IllegalArgumentException("internal exact rank-vectors query cannot be parsed from XContent");
            }),
            new QuerySpec<>(RankVectorsCandidateQueryBuilder.NAME, RankVectorsCandidateQueryBuilder::new, parser -> {
                throw new IllegalArgumentException("internal MUVERA candidate query cannot be parsed from XContent");
            })
        );
    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return List.of(new RetrieverSpec<>(RankVectorsRetrieverBuilder.NAME, RankVectorsRetrieverBuilder::fromXContent));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(RANK_VECTORS_EVAL_ENABLED, INDEX_RANK_VECTORS_EVAL_ENABLED);
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(new ActionHandler(RANK_VECTORS_EVAL_ACTION, TransportRankVectorsEvalAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestRankVectorsEvalAction());
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
