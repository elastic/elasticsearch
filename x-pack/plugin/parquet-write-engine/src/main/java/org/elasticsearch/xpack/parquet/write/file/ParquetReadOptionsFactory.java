/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

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
 * Builds {@link ParquetReadOptions} without Hadoop. The upstream builder loads Hadoop classes in its constructor (a Hadoop
 * configuration and the Hadoop codecs), and a node has no Hadoop at runtime. The constructor of the options is package-private, so this
 * class calls it through a method handle, the way the ES|QL Parquet reader plugin does for its own reads. The plugin does not extend that
 * reader plugin, because a plugin cannot extend two plugins that both contain the Parquet classes, and the Iceberg plugin that it
 * extends has them.
 */
public final class ParquetReadOptionsFactory {

    private static final MethodHandle CONSTRUCTOR;

    static {
        try {
            final var lookup = MethodHandles.privateLookupIn(ParquetReadOptions.class, MethodHandles.lookup());
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

    private ParquetReadOptionsFactory() {}

    /** The default options, which read all columns and apply no filter, with the given codecs. */
    @SuppressWarnings({ "unchecked", "cast" })
    public static ParquetReadOptions create(CompressionCodecFactory codecs) {
        try {
            return (ParquetReadOptions) CONSTRUCTOR.invokeExact(
                false,
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                false,
                (FilterCompat.Filter) null,
                (ParquetMetadataConverter.MetadataFilter) ParquetMetadataConverter.NO_FILTER,
                (CompressionCodecFactory) codecs,
                (ByteBufferAllocator) new HeapByteBufferAllocator(),
                8 * 1024 * 1024,
                (Map<String, String>) new HashMap<String, String>(),
                (FileDecryptionProperties) null,
                (ParquetMetricsCallback) null,
                (ParquetConfiguration) new PlainParquetConfiguration()
            );
        } catch (Throwable t) {
            throw new IllegalStateException("failed to construct ParquetReadOptions", t);
        }
    }
}
