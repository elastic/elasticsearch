/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetMetricsCallback;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Map;

/**
 * Hadoop-free builder for {@link ParquetReadOptions}. The upstream {@link ParquetReadOptions.Builder}
 * unconditionally loads {@code ParquetInputFormat} (which extends Hadoop's {@code FileInputFormat})
 * and {@code HadoopCodecs} in its constructor. This class uses {@link MethodHandles} to invoke the
 * package-private {@code ParquetReadOptions} constructor directly, bypassing the Builder entirely
 * and avoiding all Hadoop class loading.
 */
final class PlainParquetReadOptions {

    private static final MethodHandle CONSTRUCTOR;

    static {
        try {
            var lookup = MethodHandles.privateLookupIn(ParquetReadOptions.class, MethodHandles.lookup());
            CONSTRUCTOR = lookup.findConstructor(
                ParquetReadOptions.class,
                MethodType.methodType(
                    void.class,
                    boolean.class, // useSignedStringMinMax
                    boolean.class, // useStatsFilter
                    boolean.class, // useDictionaryFilter
                    boolean.class, // useRecordFilter
                    boolean.class, // useColumnIndexFilter
                    boolean.class, // usePageChecksumVerification
                    boolean.class, // useBloomFilter
                    boolean.class, // useOffHeapDecryptBuffer
                    boolean.class, // useHadoopVectoredIo
                    FilterCompat.Filter.class,
                    ParquetMetadataConverter.MetadataFilter.class,
                    CompressionCodecFactory.class,
                    ByteBufferAllocator.class,
                    int.class, // maxAllocationSize
                    Map.class, // properties
                    FileDecryptionProperties.class,
                    ParquetMetricsCallback.class,
                    ParquetConfiguration.class
                )
            );
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private PlainParquetReadOptions() {}

    static Builder builder(CompressionCodecFactory codecFactory) {
        return new Builder(codecFactory);
    }

    static final class Builder {
        private boolean useSignedStringMinMax = false;
        private boolean useStatsFilter = true;
        private boolean useDictionaryFilter = true;
        private boolean useRecordFilter = true;
        private boolean useColumnIndexFilter = true;
        private boolean usePageChecksumVerification = false;
        private boolean useBloomFilter = true;
        private boolean useOffHeapDecryptBuffer = false;
        private boolean useHadoopVectoredIo = false;
        private FilterCompat.Filter recordFilter = null;
        private ParquetMetadataConverter.MetadataFilter metadataFilter = ParquetMetadataConverter.NO_FILTER;
        private final CompressionCodecFactory codecFactory;
        private ByteBufferAllocator allocator = new HeapByteBufferAllocator();
        private int maxAllocationSize = 8 * 1024 * 1024;
        private final Map<String, String> properties = new HashMap<>();
        private FileDecryptionProperties fileDecryptionProperties = null;

        private Builder(CompressionCodecFactory codecFactory) {
            this.codecFactory = codecFactory;
        }

        Builder withAllocator(ByteBufferAllocator allocator) {
            this.allocator = allocator;
            return this;
        }

        Builder withRecordFilter(FilterCompat.Filter filter) {
            this.recordFilter = filter;
            return this;
        }

        Builder withRange(long start, long end) {
            this.metadataFilter = ParquetMetadataConverter.range(start, end);
            return this;
        }

        Builder withMaxAllocationInBytes(int maxAllocationSize) {
            this.maxAllocationSize = maxAllocationSize;
            return this;
        }

        @SuppressWarnings("unchecked")
        ParquetReadOptions build() {
            try {
                return (ParquetReadOptions) CONSTRUCTOR.invokeExact(
                    useSignedStringMinMax,
                    useStatsFilter,
                    useDictionaryFilter,
                    useRecordFilter,
                    useColumnIndexFilter,
                    usePageChecksumVerification,
                    useBloomFilter,
                    useOffHeapDecryptBuffer,
                    useHadoopVectoredIo,
                    (FilterCompat.Filter) recordFilter,
                    (ParquetMetadataConverter.MetadataFilter) metadataFilter,
                    (CompressionCodecFactory) codecFactory,
                    (ByteBufferAllocator) allocator,
                    maxAllocationSize,
                    (Map<String, String>) properties,
                    (FileDecryptionProperties) fileDecryptionProperties,
                    (ParquetMetricsCallback) null,
                    (ParquetConfiguration) new PlainParquetConfiguration()
                );
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to construct ParquetReadOptions", t);
            }
        }
    }
}
