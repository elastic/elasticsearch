/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugin.analysis.icu.AnalysisICUPlugin;
import org.elasticsearch.plugin.mapper.MapperMurmur3Plugin;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.List;

public class EnrichPolicyMappingsTests extends ESTestCase {

    public void testAggregationsVsTransforms() {
        // Note: if a new plugin is added, it must be added here
        IndicesModule indicesModule = new IndicesModule(
            List.of(
                new AnalysisICUPlugin(),
                new AnalyticsPlugin(),
                new AggregateMetricMapperPlugin(),
                new AnnotatedTextPlugin(),
                new ConstantKeywordMapperPlugin(),
                new MapperMurmur3Plugin(),
                new MapperSizePlugin(),
                new MapperExtrasPlugin(),
                new ParentJoinPlugin(),
                new PercolatorPlugin(),
                new Security(Settings.EMPTY),
                new SpatialPlugin(),
                new UnsignedLongMapperPlugin(),
                new VersionFieldPlugin(Settings.EMPTY),
                new Wildcard()
            )
        );
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();

        List<String> fieldTypes = mapperRegistry.getMapperParsers().keySet().stream().toList();

        for (String fieldType : fieldTypes) {
            String message = Strings.format("""
                The following field type is unknown to enrich: [%s]. If this is a newly added field type, \
                please open an issue to add enrich support for it. Afterwards add "%s" to the list in %s. \
                Thanks!\
                """, fieldType, fieldType, EnrichPolicyMappings.class.getName());
            assertTrue(
                message,
                EnrichPolicyMappings.isSupportedByEnrich(fieldType)
                    || EnrichPolicyMappings.isUnsupportedByEnrich(fieldType)
            );
        }
    }
}
