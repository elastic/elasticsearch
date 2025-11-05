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

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.index.mapper.FieldMapper.notInMultiFields;
import static org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper.CONTENT_TYPE;

/**
 * Plugin for rank vectors field mapping in Elasticsearch.
 * <p>
 * This plugin provides the {@code rank_vectors} field type, which is optimized for storing
 * and querying vectors used in ranking and similarity search operations. The field type
 * stores vectors in a memory-efficient format suitable for large-scale retrieval tasks.
 * This feature requires an Enterprise license.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * PUT /my-index
 * {
 *   "mappings": {
 *     "properties": {
 *       "embedding": {
 *         "type": "rank_vectors",
 *         "dims": 128
 *       }
 *     }
 *   }
 * }
 * }</pre>
 */
public class RankVectorsPlugin extends Plugin implements MapperPlugin {
    /**
     * Licensed feature definition for rank vectors functionality.
     * Requires an Enterprise license.
     */
    public static final LicensedFeature.Momentary RANK_VECTORS_FEATURE = LicensedFeature.momentary(
        null,
        "rank-vectors",
        License.OperationMode.ENTERPRISE
    );

    /**
     * Returns the field mappers provided by this plugin.
     * <p>
     * Registers the {@link RankVectorsFieldMapper} with license checking.
     * The mapper cannot be used in multi-fields and requires an active Enterprise license.
     * </p>
     *
     * @return a map containing the rank vectors field type parser
     */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(CONTENT_TYPE, new FieldMapper.TypeParser((n, c) -> {
            if (RANK_VECTORS_FEATURE.check(getLicenseState()) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            return new RankVectorsFieldMapper.Builder(
                n,
                c.indexVersionCreated(),
                getLicenseState(),
                INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(c.getIndexSettings().getSettings())
            );
        }, notInMultiFields(CONTENT_TYPE)));
    }

    /**
     * Returns the X-Pack license state.
     *
     * @return the shared X-Pack license state
     */
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }
}
