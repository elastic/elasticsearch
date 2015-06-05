/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.index.mapper.object.ObjectMapper;

/**
 * A holder for several {@link org.elasticsearch.index.mapper.object.ObjectMapper}.
 */
public class ObjectMappers implements Iterable<ObjectMapper> {

    private final ImmutableList<ObjectMapper> objectMappers;
    private final boolean hasNested;

    public ObjectMappers() {
        this(ImmutableList.<ObjectMapper>of());
    }

    public ObjectMappers(ObjectMapper objectMapper) {
        this(new ObjectMapper[]{objectMapper});
    }

    public ObjectMappers(ObjectMapper[] objectMappers) {
        this(ImmutableList.copyOf(objectMappers));
    }

    public ObjectMappers(ImmutableList<ObjectMapper> objectMappers) {
        this.objectMappers = objectMappers;
        boolean hasNested = false;
        for (ObjectMapper objectMapper : objectMappers) {
            if (objectMapper.nested().isNested()) {
                hasNested = true;
                break;
            }
        }
        this.hasNested = hasNested;
    }

    /**
     * Is one of the object mappers has a nested mapping set?
     */
    public boolean hasNested() {
        return this.hasNested;
    }

    public ObjectMapper mapper() {
        if (objectMappers.isEmpty()) {
            return null;
        }
        return objectMappers.get(0);
    }

    public boolean isEmpty() {
        return objectMappers.isEmpty();
    }

    public ImmutableList<ObjectMapper> mappers() {
        return this.objectMappers;
    }

    @Override
    public UnmodifiableIterator<ObjectMapper> iterator() {
        return objectMappers.iterator();
    }

    /**
     * Concats and returns a new {@link org.elasticsearch.index.mapper.ObjectMappers}.
     */
    public ObjectMappers concat(ObjectMapper mapper) {
        return new ObjectMappers(new ImmutableList.Builder<ObjectMapper>().addAll(objectMappers).add(mapper).build());
    }

    /**
     * Concats and returns a new {@link org.elasticsearch.index.mapper.ObjectMappers}.
     */
    public ObjectMappers concat(ObjectMappers mappers) {
        return new ObjectMappers(new ImmutableList.Builder<ObjectMapper>().addAll(objectMappers).addAll(mappers).build());
    }

    public ObjectMappers remove(Iterable<ObjectMapper> mappers) {
        ImmutableList.Builder<ObjectMapper> builder = new ImmutableList.Builder<>();
        for (ObjectMapper objectMapper : objectMappers) {
            boolean found = false;
            for (ObjectMapper mapper : mappers) {
                if (objectMapper == mapper) { // identify equality
                    found = true;
                }
            }
            if (!found) {
                builder.add(objectMapper);
            }
        }
        return new ObjectMappers(builder.build());
    }

    public ObjectMappers remove(ObjectMapper mapper) {
        ImmutableList.Builder<ObjectMapper> builder = new ImmutableList.Builder<>();
        for (ObjectMapper objectMapper : objectMappers) {
            if (objectMapper != mapper) { // identify equality
                builder.add(objectMapper);
            }
        }
        return new ObjectMappers(builder.build());
    }
}
