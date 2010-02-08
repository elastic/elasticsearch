/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.http.action.support;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.index.query.json.JsonQueryBuilders;
import org.elasticsearch.index.query.json.QueryStringJsonQueryBuilder;
import org.elasticsearch.util.Strings;

import java.util.regex.Pattern;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpActions {

    public final static Pattern indicesPattern;
    public final static Pattern typesPattern;
    private final static Pattern nodesIdsPattern;


    static {
        indicesPattern = Pattern.compile(",");
        typesPattern = Pattern.compile(",");
        nodesIdsPattern = Pattern.compile(",");
    }

    public static String parseQuerySource(HttpRequest request) {
        if (request.hasContent()) {
            return request.contentAsString();
        }
        String queryString = request.param("q");
        if (queryString == null) {
            throw new ElasticSearchIllegalArgumentException("No query to execute, not in body, and not bounded to 'q' parameter");
        }
        QueryStringJsonQueryBuilder queryBuilder = JsonQueryBuilders.queryString(queryString);
        queryBuilder.defaultField(request.param("df"));
        queryBuilder.analyzer(request.param("analyzer"));
        String defaultOperator = request.param("defaultOperator");
        if (defaultOperator != null) {
            if ("OR".equals(defaultOperator)) {
                queryBuilder.defualtOperator(QueryStringJsonQueryBuilder.Operator.OR);
            } else if ("AND".equals(defaultOperator)) {
                queryBuilder.defualtOperator(QueryStringJsonQueryBuilder.Operator.AND);
            } else {
                throw new ElasticSearchIllegalArgumentException("Unsupported defaultOperator [" + defaultOperator + "], can either be [OR] or [AND]");
            }
        }
        return queryBuilder.build();
    }

    public static String[] splitIndices(String indices) {
        if (indices == null) {
            return Strings.EMPTY_ARRAY;
        }
        return indicesPattern.split(indices);
    }

    public static String[] splitTypes(String typeNames) {
        return typesPattern.split(typeNames);
    }

    public static String[] splitNodes(String nodes) {
        if (nodes == null) {
            return Strings.EMPTY_ARRAY;
        }
        return nodesIdsPattern.split(nodes);
    }

    public static float paramAsFloat(String floatValue, float defaultValue) {
        if (floatValue == null) {
            return defaultValue;
        }
        return Float.parseFloat(floatValue);
    }
}
