/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.MapperPlugin;

import java.util.Map;

/**
 * Test mapper that records, in its {@code feature_seen} parameter, what {@link MapperMergeContext#clusterHasFeature} returned
 * when it was last merged. Its {@code gated} parameter can only be parsed if the cluster supports {@link #PROBE_FEATURE}.
 */
public class FeatureProbeFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "feature_probe";
    public static final NodeFeature PROBE_FEATURE = new NodeFeature("mapper.test.feature_probe");

    public static class Plugin extends org.elasticsearch.plugins.Plugin implements MapperPlugin {
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Map.of(CONTENT_TYPE, new TypeParser((n, c) -> new Builder(n)));
        }
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<String> featureSeen = Parameter.stringParam(
            "feature_seen",
            true,
            m -> ((FeatureProbeFieldMapper) m).featureSeen,
            null
        );

        private final Parameter<Boolean> gated = Parameter.boolParam("gated", true, m -> ((FeatureProbeFieldMapper) m).gated, false)
            .requiresFeatures(PROBE_FEATURE);

        public Builder(String name) {
            super(name);
        }

        public String featureSeen() {
            return featureSeen.getValue();
        }

        public boolean gated() {
            return gated.getValue();
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { featureSeen, gated };
        }

        @Override
        protected void mergeFromBuilder(FieldMapper.Builder incoming, Conflicts conflicts, MapperMergeContext mergeContext) {
            super.mergeFromBuilder(incoming, conflicts, mergeContext);
            featureSeen.setValue(Boolean.toString(mergeContext.clusterHasFeature(PROBE_FEATURE)));
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public FeatureProbeFieldMapper build(MapperBuilderContext context) {
            return new FeatureProbeFieldMapper(
                leafName(),
                new MockFieldMapper.FakeFieldType(context.buildFullName(leafName())),
                builderParams(this, context),
                featureSeen.getValue(),
                gated.getValue()
            );
        }
    }

    private final String featureSeen;
    private final boolean gated;

    private FeatureProbeFieldMapper(String simpleName, MappedFieldType fieldType, BuilderParams params, String featureSeen, boolean gated) {
        super(simpleName, fieldType, params);
        this.featureSeen = featureSeen;
        this.gated = gated;
    }

    public String featureSeen() {
        return featureSeen;
    }

    public boolean gated() {
        return gated;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName()).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {}

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
