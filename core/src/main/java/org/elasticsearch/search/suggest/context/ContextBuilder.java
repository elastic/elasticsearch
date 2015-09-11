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

package org.elasticsearch.search.suggest.context;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.DocumentMapperParser;

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class ContextBuilder<E extends ContextMapping> {

    protected String name;

    public ContextBuilder(String name) {
        this.name = name;
    }

    public abstract E build();

    /**
     * Create a new {@link GeolocationContextMapping}
     */
    public static GeolocationContextMapping.Builder location(String name) {
        return new GeolocationContextMapping.Builder(name);
    }

    /**
     * Create a new {@link GeolocationContextMapping} with given precision and
     * neighborhood usage
     * 
     * @param precision geohash length
     * @param neighbors use neighbor cells
     */
    public static GeolocationContextMapping.Builder location(String name, int precision, boolean neighbors) {
        return new GeolocationContextMapping.Builder(name, neighbors, precision);
    }

    /**
     * Create a new {@link CategoryMapping}
     */
    public static CategoryContextMapping.Builder category(String name) {
        return new CategoryContextMapping.Builder(name, null);
    }

    /**
     * Create a new {@link CategoryMapping} with default category
     * 
     * @param defaultCategory category to use, if it is not provided
     */
    public static CategoryContextMapping.Builder category(String name, String defaultCategory) {
        return new CategoryContextMapping.Builder(name, null).addDefaultValue(defaultCategory);
    }

    /**
     * Create a new {@link CategoryContextMapping}
     * 
     * @param fieldname
     *            name of the field to use
     */
    public static CategoryContextMapping.Builder reference(String name, String fieldname) {
        return new CategoryContextMapping.Builder(name, fieldname);
    }

    /**
     * Create a new {@link CategoryContextMapping}
     * 
     * @param fieldname name of the field to use
     * @param defaultValues values to use, if the document not provides
     *        a field with the given name
     */
    public static CategoryContextMapping.Builder reference(String name, String fieldname, Iterable<? extends CharSequence> defaultValues) {
        return new CategoryContextMapping.Builder(name, fieldname).addDefaultValues(defaultValues);
    }

    public static SortedMap<String, ContextMapping> loadMappings(Object configuration, Version indexVersionCreated)
            throws ElasticsearchParseException {
        if (configuration instanceof Map) {
            Map<String, Object> configurations = (Map<String, Object>)configuration;
            SortedMap<String, ContextMapping> mappings = new TreeMap<>();
            for (Entry<String,Object> config : configurations.entrySet()) {
                String name = config.getKey();
                mappings.put(name, loadMapping(name, (Map<String, Object>) config.getValue(), indexVersionCreated));
            }
            return mappings;
        } else if (configuration == null) {
            return ContextMapping.EMPTY_MAPPING;
        } else {
            throw new ElasticsearchParseException("no valid context configuration");
        }
    }

    protected static ContextMapping loadMapping(String name, Map<String, Object> config, Version indexVersionCreated)
            throws ElasticsearchParseException {
        final Object argType = config.get(ContextMapping.FIELD_TYPE);
        
        if (argType == null) {
            throw new ElasticsearchParseException("missing [{}] in context mapping", ContextMapping.FIELD_TYPE);
        }

        final String type = argType.toString(); 
        ContextMapping contextMapping;
        if (GeolocationContextMapping.TYPE.equals(type)) {
            contextMapping = GeolocationContextMapping.load(name, config);
        } else if (CategoryContextMapping.TYPE.equals(type)) {
            contextMapping = CategoryContextMapping.load(name, config);
        } else {
            throw new ElasticsearchParseException("unknown context type [{}]", type);
        }
        config.remove(ContextMapping.FIELD_TYPE);
        DocumentMapperParser.checkNoRemainingFields(name, config, indexVersionCreated);
        
        return contextMapping;
    }
}
