/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.util.concurrent.Immutable;

import java.util.List;

/**
 * A holder for several {@link FieldMapper}.
 *
 *
 */
@Immutable
public class FieldMappers implements Iterable<FieldMapper> {

    private final ImmutableList<FieldMapper> fieldMappers;

    public FieldMappers() {
        this.fieldMappers = ImmutableList.of();
    }

    public FieldMappers(FieldMapper fieldMapper) {
        this(new FieldMapper[]{fieldMapper});
    }

    public FieldMappers(FieldMapper[] fieldMappers) {
        if (fieldMappers == null) {
            fieldMappers = new FieldMapper[0];
        }
        this.fieldMappers = ImmutableList.copyOf(Iterators.forArray(fieldMappers));
    }

    public FieldMappers(ImmutableList<FieldMapper> fieldMappers) {
        this.fieldMappers = fieldMappers;
    }

    public FieldMapper mapper() {
        if (fieldMappers.isEmpty()) {
            return null;
        }
        return fieldMappers.get(0);
    }

    public boolean isEmpty() {
        return fieldMappers.isEmpty();
    }

    public ImmutableList<FieldMapper> mappers() {
        return this.fieldMappers;
    }

    @Override
    public UnmodifiableIterator<FieldMapper> iterator() {
        return fieldMappers.iterator();
    }

    /**
     * Concats and returns a new {@link FieldMappers}.
     */
    public FieldMappers concat(FieldMapper mapper) {
        return new FieldMappers(new ImmutableList.Builder<FieldMapper>().addAll(fieldMappers).add(mapper).build());
    }

    /**
     * Concats and returns a new {@link FieldMappers}.
     */
    public FieldMappers concat(FieldMappers mappers) {
        return new FieldMappers(new ImmutableList.Builder<FieldMapper>().addAll(fieldMappers).addAll(mappers).build());
    }

    public FieldMappers remove(List<FieldMapper> mappers) {
        ImmutableList.Builder<FieldMapper> builder = new ImmutableList.Builder<FieldMapper>();
        for (FieldMapper fieldMapper : fieldMappers) {
            boolean found = false;
            for (FieldMapper mapper : mappers) {
                if (fieldMapper.equals(mapper)) { // identify equality
                    found = true;
                }
            }
            if (!found) {
                builder.add(fieldMapper);
            }
        }
        return new FieldMappers(builder.build());
    }

    public FieldMappers remove(FieldMapper mapper) {
        ImmutableList.Builder<FieldMapper> builder = new ImmutableList.Builder<FieldMapper>();
        for (FieldMapper fieldMapper : fieldMappers) {
            if (!fieldMapper.equals(mapper)) { // identify equality
                builder.add(fieldMapper);
            }
        }
        return new FieldMappers(builder.build());
    }
}
