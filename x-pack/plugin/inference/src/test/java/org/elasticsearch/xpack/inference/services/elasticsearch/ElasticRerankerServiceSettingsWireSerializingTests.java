/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticRerankerServiceSettings.LongDocumentStrategy;

import java.io.IOException;
import java.util.Objects;

public class ElasticRerankerServiceSettingsWireSerializingTests extends AbstractWireSerializingTestCase<
    ElasticRerankerServiceSettingsWireSerializingTests.ElasticRerankerServiceSettingsWrapper> {

    @Override
    protected Writeable.Reader<ElasticRerankerServiceSettingsWrapper> instanceReader() {
        return ElasticRerankerServiceSettingsWrapper::new;
    }

    @Override
    protected ElasticRerankerServiceSettingsWrapper createTestInstance() {
        return new ElasticRerankerServiceSettingsWrapper(ElasticRerankerServiceSettingsTests.createRandom());
    }

    @Override
    protected ElasticRerankerServiceSettingsWrapper mutateInstance(ElasticRerankerServiceSettingsWrapper instance) {
        ElasticRerankerServiceSettings settings = instance.settings();
        return switch (between(0, 5)) {
            case 0 -> new ElasticRerankerServiceSettingsWrapper(
                copy(
                    randomValueOtherThan(settings.getNumAllocations(), () -> randomFrom(randomIntBetween(1, 10), (Integer) null)),
                    settings.getNumThreads(),
                    settings.modelId(),
                    settings.getAdaptiveAllocationsSettings(),
                    settings.getLongDocumentStrategy(),
                    settings.getMaxChunksPerDoc()
                )
            );
            case 1 -> new ElasticRerankerServiceSettingsWrapper(
                copy(
                    settings.getNumAllocations(),
                    randomValueOtherThan(settings.getNumThreads(), () -> randomIntBetween(1, 10)),
                    settings.modelId(),
                    settings.getAdaptiveAllocationsSettings(),
                    settings.getLongDocumentStrategy(),
                    settings.getMaxChunksPerDoc()
                )
            );
            case 2 -> new ElasticRerankerServiceSettingsWrapper(
                copy(
                    settings.getNumAllocations(),
                    settings.getNumThreads(),
                    randomValueOtherThan(settings.modelId(), () -> randomAlphaOfLength(8)),
                    settings.getAdaptiveAllocationsSettings(),
                    settings.getLongDocumentStrategy(),
                    settings.getMaxChunksPerDoc()
                )
            );
            case 3 -> new ElasticRerankerServiceSettingsWrapper(
                copy(
                    settings.getNumAllocations(),
                    settings.getNumThreads(),
                    settings.modelId(),
                    randomValueOtherThan(
                        settings.getAdaptiveAllocationsSettings(),
                        () -> randomFrom(AdaptiveAllocationSettingsTests.testInstance(), (AdaptiveAllocationsSettings) null)
                    ),
                    settings.getLongDocumentStrategy(),
                    settings.getMaxChunksPerDoc()
                )
            );
            case 4 -> new ElasticRerankerServiceSettingsWrapper(
                copy(
                    settings.getNumAllocations(),
                    settings.getNumThreads(),
                    settings.modelId(),
                    settings.getAdaptiveAllocationsSettings(),
                    randomValueOtherThan(
                        settings.getLongDocumentStrategy(),
                        () -> randomBoolean() ? randomFrom(LongDocumentStrategy.values()) : null
                    ),
                    settings.getMaxChunksPerDoc()
                )
            );
            case 5 -> new ElasticRerankerServiceSettingsWrapper(
                copy(
                    settings.getNumAllocations(),
                    settings.getNumThreads(),
                    settings.modelId(),
                    settings.getAdaptiveAllocationsSettings(),
                    settings.getLongDocumentStrategy(),
                    randomValueOtherThan(settings.getMaxChunksPerDoc(), () -> randomBoolean() ? randomIntBetween(1, 10) : null)
                )
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        };
    }

    private static ElasticRerankerServiceSettings copy(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc
    ) {
        return new ElasticRerankerServiceSettings(
            numAllocations,
            numThreads,
            modelId,
            adaptiveAllocationsSettings,
            longDocumentStrategy,
            maxChunksPerDoc
        );
    }

    /**
     * {@link ElasticRerankerServiceSettings} does not define equals or hashCode. The superclass compares the
     * allocation fields and ignores {@code longDocumentStrategy} and {@code maxChunksPerDoc}, which exist only
     * on this subclass. Those two fields are compared here so the wire test can see them without production
     * equality that would exist only for the test.
     */
    public static class ElasticRerankerServiceSettingsWrapper implements Writeable {

        private final ElasticRerankerServiceSettings settings;

        ElasticRerankerServiceSettingsWrapper(ElasticRerankerServiceSettings settings) {
            this.settings = settings;
        }

        ElasticRerankerServiceSettingsWrapper(StreamInput in) throws IOException {
            this(new ElasticRerankerServiceSettings(in));
        }

        ElasticRerankerServiceSettings settings() {
            return settings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            settings.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ElasticRerankerServiceSettingsWrapper that = (ElasticRerankerServiceSettingsWrapper) o;
            return Objects.equals(settings, that.settings)
                && Objects.equals(settings.getLongDocumentStrategy(), that.settings.getLongDocumentStrategy())
                && Objects.equals(settings.getMaxChunksPerDoc(), that.settings.getMaxChunksPerDoc());
        }

        @Override
        public int hashCode() {
            return Objects.hash(settings, settings.getLongDocumentStrategy(), settings.getMaxChunksPerDoc());
        }
    }
}
