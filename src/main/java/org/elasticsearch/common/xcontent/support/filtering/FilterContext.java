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

package org.elasticsearch.common.xcontent.support.filtering;

import com.fasterxml.jackson.core.JsonGenerator;
import org.elasticsearch.common.regex.Regex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A FilterContext contains the description of a field about to be written by a JsonGenerator.
 */
public class FilterContext {

    /**
     * The field/property name to be write
     */
    private String property;

    /**
     * List of XContentFilter matched by the current filtering context
     */
    private List<String[]> matchings;

    /**
     * Flag to indicate if the field/property must be written
     */
    private Boolean write = null;

    /**
     * Flag to indicate if the field/property match a filter
     */
    private boolean match = false;

    /**
     * Points to the parent context
     */
    private FilterContext parent;

    /**
     * Type of the field/property
     */
    private Type type = Type.VALUE;

    protected enum Type {
        VALUE,
        OBJECT,
        ARRAY,
        ARRAY_OF_OBJECT
    }

    public FilterContext(String property, FilterContext parent) {
        this.property = property;
        this.parent = parent;
    }

    public void reset(String property) {
        this.property = property;
        this.write = null;
        if (matchings != null) {
            matchings.clear();
        }
        this.match = false;
        this.type = Type.VALUE;
    }

    public void reset(String property, FilterContext parent) {
        reset(property);
        this.parent = parent;
        if (parent.isMatch()) {
            match = true;
        }
    }

    public FilterContext parent() {
        return parent;
    }

    public List<String[]> matchings() {
        return matchings;
    }

    public void addMatching(String[] matching) {
        if (matchings == null) {
            matchings = new ArrayList<>();
        }
        matchings.add(matching);
    }

    public boolean isRoot() {
        return parent == null;
    }

    public boolean isArray() {
        return Type.ARRAY.equals(type);
    }

    public void initArray() {
        this.type = Type.ARRAY;
    }

    public boolean isObject() {
        return Type.OBJECT.equals(type);
    }

    public void initObject() {
        this.type = Type.OBJECT;
    }

    public boolean isArrayOfObject() {
        return Type.ARRAY_OF_OBJECT.equals(type);
    }

    public void initArrayOfObject() {
        this.type = Type.ARRAY_OF_OBJECT;
    }

    public boolean isMatch() {
        return match;
    }

    /**
     * This method contains the logic to check if a field/property must be included
     * or not.
     */
    public boolean include() {
        if (write == null) {
            if (parent != null) {
                // the parent context matches the end of a filter list:
                // by default we include all the sub properties so we
                // don't need to check if the sub properties also match
                if (parent.isMatch()) {
                    write = true;
                    match = true;
                    return write;
                }

                if (parent.matchings() != null) {

                    // Iterates over the filters matched by the parent context
                    // and checks if the current context also match
                    for (String[] matcher : parent.matchings()) {
                        if (matcher.length > 0) {
                            String field = matcher[0];

                            if ("**".equals(field)) {
                                addMatching(matcher);
                            }

                            if ((field != null) && (Regex.simpleMatch(field, property))) {
                                int remaining = matcher.length - 1;

                                // the current context matches the end of a filter list:
                                // it must be written and it is flagged as a direct match
                                if (remaining == 0) {
                                    write = true;
                                    match = true;
                                    return write;
                                } else {
                                    String[] submatching = new String[remaining];
                                    System.arraycopy(matcher, 1, submatching, 0, remaining);
                                    addMatching(submatching);
                                }
                            }
                        }
                    }
                }
            } else {
                // Root object is always written
                write = true;
            }

            if (write == null) {
                write = false;
            }
        }
        return write;
    }

    /**
     * Ensure that the full path to the current field is write by the JsonGenerator
     *
     * @param generator
     * @throws IOException
     */
    public void writePath(JsonGenerator generator) throws IOException {
        if (parent != null) {
            parent.writePath(generator);
        }

        if ((write == null) || (!write)) {
            write = true;

            if (property == null) {
                generator.writeStartObject();
            } else {
                generator.writeFieldName(property);
                if (isArray()) {
                    generator.writeStartArray();
                } else if (isObject() || isArrayOfObject()) {
                    generator.writeStartObject();
                }
            }
        }
    }
}
