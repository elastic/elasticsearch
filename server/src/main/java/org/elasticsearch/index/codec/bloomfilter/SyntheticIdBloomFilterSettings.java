/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Holds the index-level settings that configure the bloom filter used for the synthetic {@code _id} field.
 * All settings are only permitted when {@link IndexSettings#SYNTHETIC_ID} is enabled, and are therefore
 * registered conditionally on {@link IndexSettings#TSDB_SYNTHETIC_ID_FEATURE_FLAG}.
 */
public final class SyntheticIdBloomFilterSettings {

    public static final Setting<Integer> NUM_HASH_FUNCTIONS = Setting.intSetting(
        "index.synthetic_id.bloom_filter.num_hash_functions",
        ES94BloomFilterDocValuesFormat.DEFAULT_NUM_HASH_FUNCTIONS,
        1,
        requiresSyntheticIdValidator("index.synthetic_id.bloom_filter.num_hash_functions", value -> {
            if (value > ES94BloomFilterDocValuesFormat.MAX_NUM_HASH_FUNCTIONS) {
                throw new IllegalArgumentException(
                    "Failed to parse value ["
                        + value
                        + "] for setting [index.synthetic_id.bloom_filter.num_hash_functions] must be <= "
                        + ES94BloomFilterDocValuesFormat.MAX_NUM_HASH_FUNCTIONS
                );
            }
        }),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<Integer> SMALL_SEGMENT_MAX_DOCS = Setting.intSetting(
        "index.synthetic_id.bloom_filter.small_segment_max_docs",
        ES94BloomFilterDocValuesFormat.DEFAULT_SMALL_SEGMENT_MAX_DOCS,
        ES94BloomFilterDocValuesFormat.MIN_SEGMENT_DOCS,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings, boolean isPresent) {
                int largeSegmentMinDocs = (Integer) settings.get(LARGE_SEGMENT_MIN_DOCS);
                if (value > largeSegmentMinDocs) {
                    throw new IllegalArgumentException(
                        "The setting [index.synthetic_id.bloom_filter.small_segment_max_docs] ("
                            + value
                            + ") must be less than or equal to [index.synthetic_id.bloom_filter.large_segment_min_docs] ("
                            + largeSegmentMinDocs
                            + ")."
                    );
                }
                checkSyntheticIdEnabled("index.synthetic_id.bloom_filter.small_segment_max_docs", settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(IndexSettings.SYNTHETIC_ID, LARGE_SEGMENT_MIN_DOCS).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<Integer> LARGE_SEGMENT_MIN_DOCS = Setting.intSetting(
        "index.synthetic_id.bloom_filter.large_segment_min_docs",
        ES94BloomFilterDocValuesFormat.DEFAULT_LARGE_SEGMENT_MIN_DOCS,
        ES94BloomFilterDocValuesFormat.MIN_SEGMENT_DOCS,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings, boolean isPresent) {
                int smallSegmentMaxDocs = (Integer) settings.get(SMALL_SEGMENT_MAX_DOCS);
                if (value < smallSegmentMaxDocs) {
                    throw new IllegalArgumentException(
                        "The setting [index.synthetic_id.bloom_filter.large_segment_min_docs] ("
                            + value
                            + ") must be greater than or equal to [index.synthetic_id.bloom_filter.small_segment_max_docs] ("
                            + smallSegmentMaxDocs
                            + ")."
                    );
                }
                checkSyntheticIdEnabled("index.synthetic_id.bloom_filter.large_segment_min_docs", settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(IndexSettings.SYNTHETIC_ID, SMALL_SEGMENT_MAX_DOCS).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<Double> HIGH_BITS_PER_DOC = new Setting<>(
        "index.synthetic_id.bloom_filter.high_bits_per_doc",
        Double.toString(ES94BloomFilterDocValuesFormat.DEFAULT_HIGH_BITS_PER_DOC),
        s -> Setting.parseDouble(
            s,
            ES94BloomFilterDocValuesFormat.MIN_BITS_PER_DOC,
            ES94BloomFilterDocValuesFormat.MAX_BITS_PER_DOC,
            "index.synthetic_id.bloom_filter.high_bits_per_doc",
            false
        ),
        new Setting.Validator<>() {
            @Override
            public void validate(Double value) {}

            @Override
            public void validate(Double value, Map<Setting<?>, Object> settings, boolean isPresent) {
                double lowBitsPerDoc = (Double) settings.get(LOW_BITS_PER_DOC);
                if (value < lowBitsPerDoc) {
                    throw new IllegalArgumentException(
                        "The setting [index.synthetic_id.bloom_filter.high_bits_per_doc] ("
                            + value
                            + ") must be >= [index.synthetic_id.bloom_filter.low_bits_per_doc] ("
                            + lowBitsPerDoc
                            + ")."
                    );
                }
                checkSyntheticIdEnabled("index.synthetic_id.bloom_filter.high_bits_per_doc", settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(IndexSettings.SYNTHETIC_ID, LOW_BITS_PER_DOC).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<Double> LOW_BITS_PER_DOC = new Setting<>(
        "index.synthetic_id.bloom_filter.low_bits_per_doc",
        Double.toString(ES94BloomFilterDocValuesFormat.DEFAULT_LOW_BITS_PER_DOC),
        s -> Setting.parseDouble(
            s,
            ES94BloomFilterDocValuesFormat.MIN_BITS_PER_DOC,
            ES94BloomFilterDocValuesFormat.MAX_BITS_PER_DOC,
            "index.synthetic_id.bloom_filter.low_bits_per_doc",
            false
        ),
        new Setting.Validator<>() {
            @Override
            public void validate(Double value) {}

            @Override
            public void validate(Double value, Map<Setting<?>, Object> settings, boolean isPresent) {
                double highBitsPerDoc = (Double) settings.get(HIGH_BITS_PER_DOC);
                if (value > highBitsPerDoc) {
                    throw new IllegalArgumentException(
                        "The setting [index.synthetic_id.bloom_filter.low_bits_per_doc] ("
                            + value
                            + ") must be <= [index.synthetic_id.bloom_filter.high_bits_per_doc] ("
                            + highBitsPerDoc
                            + ")."
                    );
                }
                checkSyntheticIdEnabled("index.synthetic_id.bloom_filter.low_bits_per_doc", settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(IndexSettings.SYNTHETIC_ID, HIGH_BITS_PER_DOC).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<ByteSizeValue> MAX_SIZE = new Setting<>(
        "index.synthetic_id.bloom_filter.max_size",
        ES94BloomFilterDocValuesFormat.MAX_BLOOM_FILTER_SIZE.getStringRep(),
        s -> Setting.parseByteSize(
            s,
            ES94BloomFilterDocValuesFormat.MIN_BLOOM_FILTER_SIZE,
            ES94BloomFilterDocValuesFormat.MAX_BLOOM_FILTER_SIZE,
            "index.synthetic_id.bloom_filter.max_size"
        ),
        requiresSyntheticIdValidator("index.synthetic_id.bloom_filter.max_size"),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<Boolean> OPTIMIZED_MERGE = Setting.boolSetting(
        "index.synthetic_id.bloom_filter.optimized_merge",
        true,
        requiresSyntheticIdValidator("index.synthetic_id.bloom_filter.optimized_merge"),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    private final int numHashFunctions;
    private final int smallSegmentMaxDocs;
    private final int largeSegmentMinDocs;
    private final double highBitsPerDoc;
    private final double lowBitsPerDoc;
    private final ByteSizeValue maxSize;
    private final boolean optimizedMerge;

    private SyntheticIdBloomFilterSettings(
        int numHashFunctions,
        int smallSegmentMaxDocs,
        int largeSegmentMinDocs,
        double highBitsPerDoc,
        double lowBitsPerDoc,
        ByteSizeValue maxSize,
        boolean optimizedMerge
    ) {
        this.numHashFunctions = numHashFunctions;
        this.smallSegmentMaxDocs = smallSegmentMaxDocs;
        this.largeSegmentMinDocs = largeSegmentMinDocs;
        this.highBitsPerDoc = highBitsPerDoc;
        this.lowBitsPerDoc = lowBitsPerDoc;
        this.maxSize = maxSize;
        this.optimizedMerge = optimizedMerge;
    }

    public static SyntheticIdBloomFilterSettings fromScopedSettings(AbstractScopedSettings scopedSettings) {
        return new SyntheticIdBloomFilterSettings(
            scopedSettings.get(NUM_HASH_FUNCTIONS),
            scopedSettings.get(SMALL_SEGMENT_MAX_DOCS),
            scopedSettings.get(LARGE_SEGMENT_MIN_DOCS),
            scopedSettings.get(HIGH_BITS_PER_DOC),
            scopedSettings.get(LOW_BITS_PER_DOC),
            scopedSettings.get(MAX_SIZE),
            scopedSettings.get(OPTIMIZED_MERGE)
        );
    }

    public int numHashFunctions() {
        return numHashFunctions;
    }

    public int smallSegmentMaxDocs() {
        return smallSegmentMaxDocs;
    }

    public int largeSegmentMinDocs() {
        return largeSegmentMinDocs;
    }

    public double highBitsPerDoc() {
        return highBitsPerDoc;
    }

    public double lowBitsPerDoc() {
        return lowBitsPerDoc;
    }

    public ByteSizeValue maxSize() {
        return maxSize;
    }

    public boolean optimizedMerge() {
        return optimizedMerge;
    }

    private static <T> Setting.Validator<T> requiresSyntheticIdValidator(String settingKey) {
        return requiresSyntheticIdValidator(settingKey, value -> {});
    }

    private static <T> Setting.Validator<T> requiresSyntheticIdValidator(String settingKey, Consumer<T> additionalValidation) {
        return new Setting.Validator<>() {
            @Override
            public void validate(T value) {
                additionalValidation.accept(value);
            }

            @Override
            public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
                validate(value);
                checkSyntheticIdEnabled(settingKey, settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(IndexSettings.SYNTHETIC_ID).iterator();
            }
        };
    }

    private static void checkSyntheticIdEnabled(String settingKey, Map<Setting<?>, Object> settings, boolean isPresent) {
        if (isPresent) {
            Boolean syntheticId = (Boolean) settings.get(IndexSettings.SYNTHETIC_ID);
            if (syntheticId == null || syntheticId == false) {
                throw new IllegalArgumentException(
                    "The setting [" + settingKey + "] can only be set when [" + IndexSettings.SYNTHETIC_ID.getKey() + "] is enabled."
                );
            }
        }
    }
}
