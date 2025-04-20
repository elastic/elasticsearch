/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors;

import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;

import java.util.Map;

import static org.elasticsearch.index.mapper.FieldMapper.notInMultiFields;
import static org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper.CONTENT_TYPE;

public class RankVectorsPlugin extends Plugin implements MapperPlugin {
    public static final LicensedFeature.Momentary RANK_VECTORS_FEATURE = LicensedFeature.momentary(
        null,
        "rank-vectors",
        License.OperationMode.ENTERPRISE
    );

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(CONTENT_TYPE, new FieldMapper.TypeParser((n, c) -> {
            if (RANK_VECTORS_FEATURE.check(getLicenseState()) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            return new RankVectorsFieldMapper.Builder(n, c.indexVersionCreated(), getLicenseState());
        }, notInMultiFields(CONTENT_TYPE)));
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
