/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MapperMergeContext {

    private final MapperBuilderContext mapperBuilderContext;
    private final AtomicLong remainingFieldsUntilLimit;
    private final ObjectMapper.Dynamic dynamic;
    private final boolean autoUpdate;

    /**
     * The root context, to be used when building a tree of mappers
     */
    public static MapperMergeContext root(
        boolean isSourceSynthetic,
        boolean isDataStream,
        long remainingFieldsUntilLimit,
        ObjectMapper.Dynamic dynamic,
        boolean autoUpdate
    ) {
        return new MapperMergeContext(
            MapperBuilderContext.root(isSourceSynthetic, isDataStream),
            new AtomicLong(remainingFieldsUntilLimit),
            dynamic,
            autoUpdate
        );
    }

    /**
     * This mapper merge context will not support limiting the number of fields during the merge process.
     * Therefore, it's not appropriate to use this in the context of an auto-update aka dynamic mapping update.
     * Outside of that context, this is safe, though.
     */
    public static MapperMergeContext root(boolean isSourceSynthetic, boolean isDataStream) {
        return from(MapperBuilderContext.root(isSourceSynthetic, isDataStream));
    }

    /**
     * This mapper merge context will not support limiting the number of fields during the merge process.
     * Therefore, it's not appropriate to use this in the context of an auto-update aka dynamic mapping update.
     * Outside of that context, this is safe, though.
     */
    public static MapperMergeContext from(MapperBuilderContext mapperBuilderContext) {
        return new MapperMergeContext(mapperBuilderContext, new AtomicLong(0), null, false);
    }

    protected MapperMergeContext(
        MapperBuilderContext mapperBuilderContext,
        AtomicLong remainingFieldsUntilLimit,
        ObjectMapper.Dynamic dynamic,
        boolean autoUpdate
    ) {
        this.mapperBuilderContext = mapperBuilderContext;
        this.remainingFieldsUntilLimit = remainingFieldsUntilLimit;
        this.dynamic = dynamic;
        this.autoUpdate = autoUpdate;
    }

    public MapperMergeContext createChildContext(String name, ObjectMapper.Dynamic dynamic) {
        return createChildContext(mapperBuilderContext.createChildContext(name), dynamic);
    }

    public MapperMergeContext createChildContext(MapperBuilderContext childContext, ObjectMapper.Dynamic dynamic) {
        return new MapperMergeContext(childContext, remainingFieldsUntilLimit, dynamic == null ? this.dynamic : dynamic, autoUpdate);
    }

    public MapperBuilderContext getMapperBuilderContext() {
        return mapperBuilderContext;
    }

    public void removeRuntimeField(Map<String, RuntimeField> runtimeFields, String name) {
        if (runtimeFields.containsKey(name)) {
            runtimeFields.remove(name);
            if (remainingFieldsUntilLimit.get() != Long.MAX_VALUE) {
                remainingFieldsUntilLimit.incrementAndGet();
            }
        }
    }

    public void addRuntimeFieldIfPossible(Map<String, RuntimeField> runtimeFields, RuntimeField runtimeField) {
        if (runtimeFields.containsKey(runtimeField.name())) {
            runtimeFields.put(runtimeField.name(), runtimeField);
        } else if (canAddField(1)) {
            remainingFieldsUntilLimit.decrementAndGet();
            runtimeFields.put(runtimeField.name(), runtimeField);
        }
    }

    public boolean addFieldIfPossible(Map<String, Mapper> mappers, Mapper mapper) {
        if (canAddField(mapper.mapperSize())) {
            remainingFieldsUntilLimit.getAndAdd(mapper.mapperSize() * -1);
            mappers.put(mapper.simpleName(), mapper);
            return true;
        }
        return false;
    }

    public void addFieldIfPossible(Mapper mapper, Runnable addField) {
        if (canAddField(mapper.mapperSize())) {
            remainingFieldsUntilLimit.getAndAdd(mapper.mapperSize() * -1);
            addField.run();
        }
    }

    public boolean canAddField(int fieldSize) {
        return autoUpdate == false || dynamic != ObjectMapper.Dynamic.TRUE_UNTIL_LIMIT || remainingFieldsUntilLimit.get() >= fieldSize;
    }
}
