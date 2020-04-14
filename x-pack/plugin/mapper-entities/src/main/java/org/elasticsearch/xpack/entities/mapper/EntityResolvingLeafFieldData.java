/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.entities.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;

import java.util.function.Function;

public class EntityResolvingLeafFieldData extends AbstractLeafOrdinalsFieldData {

    private final String field;
    private final LeafReader leafReader;
    private final EntityMap entities;

    public EntityResolvingLeafFieldData(Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction, String field, LeafReader leafReader, EntityMap entities) {
        super(scriptFunction);
        this.field = field;
        this.leafReader = leafReader;
        this.entities = entities;
    }

    @Override
    public SortedSetDocValues getOrdinalsValues() {
        return null;
        // we need to do two things here:
        //      - merge the backing SortedSetDocValues with the entities from EntityMap
        //      - map ordinals from synonyms in the EntityMap to their target entity ordinal in the merged docvalues
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public void close() {

    }
}
