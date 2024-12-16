/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;

import java.util.Map;

public class RankVectorsPlugin extends Plugin implements MapperPlugin {
    public static final LicensedFeature.Momentary RANK_VECTORS_FEATURE = LicensedFeature.momentary(
        null,
        "rank-vectors",
        License.OperationMode.ENTERPRISE
    );

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(RankVectorsFieldMapper.CONTENT_TYPE, RankVectorsFieldMapper.PARSER);
    }
}
