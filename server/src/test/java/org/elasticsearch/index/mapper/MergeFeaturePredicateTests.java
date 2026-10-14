/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that the {@link MapperService} merge paths pass the cluster feature predicate to {@link MapperMergeContext}.
 */
public class MergeFeaturePredicateTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new FeatureProbeFieldMapper.Plugin());
    }

    public void testMappingUpdateUsesClusterFeatures() throws IOException {
        AtomicBoolean supported = new AtomicBoolean(false);
        MapperService mapperService = probeService(supported, probesAtEveryLevel("unset"));

        merge(mapperService, MergeReason.MAPPING_UPDATE, probesAtEveryLevel(null));
        assertProbesAtEveryLevel(mapperService, "false");

        // the predicate is consulted on each merge
        supported.set(true);
        merge(mapperService, MergeReason.MAPPING_UPDATE, probesAtEveryLevel(null));
        assertProbesAtEveryLevel(mapperService, "true");
    }

    public void testMappingRecoveryAssumesAllFeatures() throws IOException {
        MapperService mapperService = probeService(new AtomicBoolean(false), probesAtEveryLevel("unset"));
        merge(mapperService, MergeReason.MAPPING_RECOVERY, probesAtEveryLevel(null));

        assertProbesAtEveryLevel(mapperService, "true");
    }

    public void testDedupMergeUseClusterFeatures() throws IOException {
        MapperService mapperService = probeService(new AtomicBoolean(false), null);
        merge(mapperService, MergeReason.MAPPING_UPDATE, probesAtEveryLevel(null));

        // obj.p is defined twice, so only the dedup merge sets feature_seen
        assertThat(featureSeen(mapperService, "obj.p"), equalTo("false"));
        // probes without duplicates are not merged, which shows the value above comes from dedup
        assertThat(featureSeen(mapperService, "p"), nullValue());
        assertThat(featureSeen(mapperService, "n.p"), nullValue());
        assertThat(featureSeen(mapperService, "k.p"), nullValue());
    }

    public void testIsNoOpUpdateUsesClusterFeatures() throws IOException {
        AtomicBoolean supported = new AtomicBoolean(false);
        MapperService mapperService = probeService(supported, simpleProbe("unset"));

        merge(mapperService, MergeReason.MAPPING_UPDATE, simpleProbe(null));
        assertSimpleProbe(mapperService, "false");
        assertTrue(mapperService.isNoOpUpdate(compressed(simpleProbe(null))));

        supported.set(true);
        assertFalse(mapperService.isNoOpUpdate(compressed(simpleProbe(null))));
    }

    public void testPreviewDynamicMappingUpdateUsesClusterFeatures() throws IOException {
        MapperService mapperService = probeService(new AtomicBoolean(false), simpleProbe("unset"));
        DocumentMapper preview = mapperService.previewDynamicMappingUpdate(compressed(simpleProbe(null)));
        assertThat(featureSeen(preview, "p"), equalTo("false"));
    }

    public void testDynamicUpdateMergerUsesClusterFeatures() throws IOException {
        // p must not be mapped yet, otherwise the merger refuses to combine updates
        MapperService mapperService = probeService(new AtomicBoolean(false), null);
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(compressed(simpleProbe(null)));
        assertTrue(merger.add(compressed(simpleProbe(null, b -> b.startObject("q").field("type", "keyword").endObject()))));
        assertThat(merger.merged().string(), containsString("\"feature_seen\":\"false\""));

        // the update fails to parse, which drops the combined builder so that merged() rebuilds it from the accepted updates
        assertFalse(merger.add(compressed(mapping(b -> b.startObject("r").field("type", "no_such_type").endObject()))));
        assertThat(merger.merged().string(), containsString("\"feature_seen\":\"false\""));
    }

    private MapperService probeService(AtomicBoolean supported, @Nullable XContentBuilder initialMapping) throws IOException {
        MapperService mapperService = new TestMapperServiceBuilder().clusterSupportsFeature(f -> supported.get()).build();
        if (initialMapping != null) {
            merge(mapperService, initialMapping);
        }
        return mapperService;
    }

    private static XContentBuilder simpleProbe(@Nullable String featureSeen) throws IOException {
        return simpleProbe(featureSeen, null);
    }

    private static XContentBuilder simpleProbe(
        @Nullable String featureSeen,
        @Nullable CheckedConsumer<XContentBuilder, IOException> additionalFieldBuilder
    ) throws IOException {
        return mapping(b -> {
            addProbeField(b, "p", featureSeen);
            if (additionalFieldBuilder != null) {
                additionalFieldBuilder.accept(b);
            }
        });
    }

    /**
     * Defines the probe at the root, in an object, in a nested field, and as a multi-field.
     */
    private static XContentBuilder probesAtEveryLevel(@Nullable String featureSeen) throws IOException {
        return mapping(b -> {
            addProbeField(b, "p", featureSeen);
            b.startObject("obj").startObject("properties");
            addProbeField(b, "p", featureSeen);
            b.endObject().endObject();
            addProbeField(b, "obj.p", featureSeen);
            b.startObject("n").field("type", "nested").startObject("properties");
            addProbeField(b, "p", featureSeen);
            b.endObject().endObject();
            b.startObject("k").field("type", "keyword").startObject("fields");
            addProbeField(b, "p", featureSeen);
            b.endObject().endObject();
        });
    }

    /** Adds a {@code feature_probe} field, setting {@code feature_seen} only when {@code featureSeen} is non-null. */
    private static void addProbeField(XContentBuilder b, String name, @Nullable String featureSeen) throws IOException {
        b.startObject(name).field("type", FeatureProbeFieldMapper.CONTENT_TYPE);
        if (featureSeen != null) {
            b.field("feature_seen", featureSeen);
        }
        b.endObject();
    }

    private static CompressedXContent compressed(XContentBuilder mapping) throws IOException {
        return new CompressedXContent(BytesReference.bytes(mapping));
    }

    private static void assertSimpleProbe(MapperService mapperService, String expected) {
        assertThat(featureSeen(mapperService, "p"), equalTo(expected));
    }

    private static void assertProbesAtEveryLevel(MapperService mapperService, String expected) {
        assertThat(featureSeen(mapperService, "p"), equalTo(expected));
        assertThat(featureSeen(mapperService, "obj.p"), equalTo(expected));
        assertThat(featureSeen(mapperService, "n.p"), equalTo(expected));
        assertThat(featureSeen(mapperService, "k.p"), equalTo(expected));
    }

    private static String featureSeen(MapperService mapperService, String path) {
        return featureSeen(mapperService.documentMapper(), path);
    }

    private static String featureSeen(DocumentMapper documentMapper, String path) {
        Mapper mapper = documentMapper.mappers().getMapper(path);
        assertThat(mapper, instanceOf(FeatureProbeFieldMapper.class));

        FeatureProbeFieldMapper featureProbeFieldMapper = (FeatureProbeFieldMapper) mapper;
        return featureProbeFieldMapper.featureSeen();
    }
}
