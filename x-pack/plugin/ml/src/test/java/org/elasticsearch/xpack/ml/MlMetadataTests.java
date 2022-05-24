/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlMetadata;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MlMetadataTests extends AbstractSerializingTestCase<MlMetadata> {

    public static MlMetadata randomInstance() {
        MlMetadata.Builder builder = new MlMetadata.Builder();
        Long biggestMlNodeSeen = randomBoolean() ? null : randomLongBetween(-1L, Long.MAX_VALUE / 2);
        return builder.isResetMode(randomBoolean())
            .isUpgradeMode(randomBoolean())
            .setMaxMlNodeSeen(biggestMlNodeSeen)
            .setCpuRatio(biggestMlNodeSeen != null && biggestMlNodeSeen > 0 ? randomDouble() : null)
            .build();
    }

    @Override
    protected MlMetadata createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<MlMetadata> instanceReader() {
        return MlMetadata::new;
    }

    @Override
    protected MlMetadata doParseInstance(XContentParser parser) {
        return MlMetadata.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testBuilderClone() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            MlMetadata first = createTestInstance();
            MlMetadata cloned = MlMetadata.Builder.from(first).build();
            assertThat(cloned, equalTo(first));
        }
    }

    @Override
    protected MlMetadata mutateInstance(MlMetadata instance) {
        boolean isUpgrade = instance.isUpgradeMode();
        boolean isReset = instance.isResetMode();
        long biggestNode = instance.getMaxMlNodeSeen();
        MlMetadata.Builder metadataBuilder = new MlMetadata.Builder(instance);

        switch (between(0, 2)) {
            case 0 -> metadataBuilder.isUpgradeMode(isUpgrade == false);
            case 1 -> metadataBuilder.isResetMode(isReset == false);
            case 2 -> {
                long biggestMlNodeSeen = biggestNode > 0 ? 0 : randomLongBetween(1, Long.MAX_VALUE / 2);
                metadataBuilder.setMaxMlNodeSeen(biggestMlNodeSeen);
                if (biggestMlNodeSeen > 0) {
                    metadataBuilder.setCpuRatio(randomDouble());
                } else {
                    metadataBuilder.setCpuRatio(null);
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return metadataBuilder.build();
    }
}
