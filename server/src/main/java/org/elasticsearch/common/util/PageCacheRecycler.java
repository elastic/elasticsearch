/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.recycler.AbstractRecyclerC;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.common.recycler.Recyclers.concurrent;
import static org.elasticsearch.common.recycler.Recyclers.concurrentDeque;
import static org.elasticsearch.common.recycler.Recyclers.dequeFactory;
import static org.elasticsearch.common.recycler.Recyclers.none;

/** A recycler of fixed-size pages. */
public class PageCacheRecycler {

    public static final Setting<Type> TYPE_SETTING = new Setting<>(
        "cache.recycler.page.type",
        Type.CONCURRENT.name(),
        Type::parse,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> LIMIT_HEAP_SETTING = Setting.memorySizeSetting(
        "cache.recycler.page.limit.heap",
        "10%",
        Property.NodeScope
    );
    public static final Setting<Double> WEIGHT_BYTES_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.bytes",
        1d,
        0d,
        Property.NodeScope
    );
    public static final Setting<Double> WEIGHT_LONG_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.longs",
        1d,
        0d,
        Property.NodeScope,
        Property.DeprecatedWarning
    );
    public static final Setting<Double> WEIGHT_INT_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.ints",
        1d,
        0d,
        Property.NodeScope,
        Property.DeprecatedWarning
    );
    // object pages are less useful to us so we give them a lower weight by default
    public static final Setting<Double> WEIGHT_OBJECTS_SETTING = Setting.doubleSetting(
        "cache.recycler.page.weight.objects",
        0.1d,
        0d,
        Property.NodeScope
    );

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int OBJECT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    public static final int LONG_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Long.BYTES;
    public static final int INT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Integer.BYTES;
    public static final int FLOAT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Float.BYTES;
    public static final int DOUBLE_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Double.BYTES;
    public static final int BYTE_PAGE_SIZE = PAGE_SIZE_IN_BYTES;

    private final Recycler<byte[]> bytePage;
    private final Recycler<Object[]> objectPage;

    public static final PageCacheRecycler NON_RECYCLING_INSTANCE;

    static {
        NON_RECYCLING_INSTANCE = new PageCacheRecycler(Settings.builder().put(LIMIT_HEAP_SETTING.getKey(), "0%").build());
    }

    public PageCacheRecycler(Settings settings) {
        final Type type = TYPE_SETTING.get(settings);
        final long limit = LIMIT_HEAP_SETTING.get(settings).getBytes();
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);

        // We have a global amount of memory that we need to divide across data types.
        // Since some types are more useful than other ones we give them different weights.
        final double bytesWeight = WEIGHT_BYTES_SETTING.get(settings);
        final double objectsWeight = WEIGHT_OBJECTS_SETTING.get(settings);

        final double totalWeight = bytesWeight + objectsWeight;
        final int maxPageCount = (int) Math.min(Integer.MAX_VALUE, limit / PAGE_SIZE_IN_BYTES);

        final int maxBytePageCount = (int) (bytesWeight * maxPageCount / totalWeight);
        bytePage = build(type, maxBytePageCount, allocatedProcessors, new AbstractRecyclerC<byte[]>() {
            @Override
            public byte[] newInstance() {
                return new byte[BYTE_PAGE_SIZE];
            }

            @Override
            public void recycle(byte[] value) {
                // nothing to do
            }
        });

        final int maxObjectPageCount = (int) (objectsWeight * maxPageCount / totalWeight);
        objectPage = build(type, maxObjectPageCount, allocatedProcessors, new AbstractRecyclerC<Object[]>() {
            @Override
            public Object[] newInstance() {
                return new Object[OBJECT_PAGE_SIZE];
            }

            @Override
            public void recycle(Object[] value) {
                Arrays.fill(value, null); // we need to remove the strong refs on the objects stored in the array
            }
        });

        assert PAGE_SIZE_IN_BYTES * (maxBytePageCount + maxObjectPageCount) <= limit;
    }

    public Recycler.V<byte[]> bytePage(boolean clear) {
        final Recycler.V<byte[]> v = bytePage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), (byte) 0);
        }
        return v;
    }

    public Recycler.V<Object[]> objectPage() {
        // object pages are cleared on release anyway
        return objectPage.obtain();
    }

    private static <T> Recycler<T> build(Type type, int limit, int availableProcessors, Recycler.C<T> c) {
        final Recycler<T> recycler;
        if (limit == 0) {
            recycler = none(c);
        } else {
            recycler = type.build(c, limit, availableProcessors);
        }
        return recycler;
    }

    public enum Type {
        QUEUE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrentDeque(c, limit);
            }
        },
        CONCURRENT {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrent(dequeFactory(c, limit / availableProcessors), availableProcessors);
            }
        },
        NONE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return none(c);
            }
        };

        public static Type parse(String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("no type support [" + type + "]");
            }
        }

        abstract <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors);
    }
}
