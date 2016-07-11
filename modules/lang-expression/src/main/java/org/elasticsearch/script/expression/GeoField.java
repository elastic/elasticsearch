package org.elasticsearch.script.expression;

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

import org.apache.lucene.queries.function.ValueSource;
import org.elasticsearch.index.fielddata.IndexFieldData;

/**
 * Expressions API for geo_point fields.
 */
final class GeoField {
    // no instance
    private GeoField() {}
    
    // supported variables
    static final String EMPTY_VARIABLE        = "empty";
    static final String LAT_VARIABLE          = "lat";
    static final String LON_VARIABLE          = "lon";
    
    // supported methods
    static final String ISEMPTY_METHOD        = "isEmpty";
    static final String GETLAT_METHOD         = "getLat";
    static final String GETLON_METHOD         = "getLon";
    
    static ValueSource getVariable(IndexFieldData<?> fieldData, String fieldName, String variable) {
        switch (variable) {
            case EMPTY_VARIABLE:
                return new GeoEmptyValueSource(fieldData);
            case LAT_VARIABLE:
                return new GeoLatitudeValueSource(fieldData);
            case LON_VARIABLE:
                return new GeoLongitudeValueSource(fieldData);
            default:
                throw new IllegalArgumentException("Member variable [" + variable + "] does not exist for geo field [" + fieldName + "].");
        }
    }
    
    static ValueSource getMethod(IndexFieldData<?> fieldData, String fieldName, String method) {
        switch (method) {
            case ISEMPTY_METHOD:
                return new GeoEmptyValueSource(fieldData);
            case GETLAT_METHOD:
                return new GeoLatitudeValueSource(fieldData);
            case GETLON_METHOD:
                return new GeoLongitudeValueSource(fieldData);
            default:
                throw new IllegalArgumentException("Member method [" + method + "] does not exist for geo field [" + fieldName + "].");
        }
    }
}
