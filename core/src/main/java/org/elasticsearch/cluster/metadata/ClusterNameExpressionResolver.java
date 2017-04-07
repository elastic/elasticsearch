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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.search.RemoteClusterConnection;
import org.elasticsearch.action.search.RemoteClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterNameExpressionResolver extends AbstractComponent {

    public ClusterNameExpressionResolver(Settings settings) {
        super(settings);
        new WildcardExpressionResolver();
    }

    private class WildcardExpressionResolver {

        private List<String> resolve(Map<String, RemoteClusterConnection> remoteClusters, List<String> expressions) {
            if (isEmptyOrTrivialWildcard(expressions)) {
                return resolveEmptyOrTrivialWildcard(remoteClusters);
            }

            List<String> names = innerResolve(remoteClusters, expressions);

            if (names.isEmpty()) {
                // TODO: Do I need to handle?
            }

            return names;
        }

        private  List<String> innerResolve(Map<String, RemoteClusterConnection> remoteClusters, List<String> expressions) {
            Set<String> result = new HashSet<>();

            boolean wildcardSeen = false;
            for (int i = 0; i < expressions.size(); i++) {
                String expression = expressions.get(i);
                if (remoteClusters.containsKey(expression)) {
                    if (result != null) {
                        result.add(expression);
                    }
                    continue;
                }
                if (Strings.isEmpty(expression)) {
                    // In the current system I believe we do nothing if a Cluster is not found
//                    throw infe(expression);
                }
                boolean add = true;
//                if (expression.charAt(0) == '+') {
//                    // if its the first, add empty result set
//                    if (i == 0) {
//                        result = new HashSet<>();
//                    }
//                    expression = expression.substring(1);
//                } else if (expression.charAt(0) == '-') {
//                    // if there is a negation without a wildcard being previously seen, add it verbatim,
//                    // otherwise return the expression
//                    if (wildcardSeen) {
//                        add = false;
//                        expression = expression.substring(1);
//                    } else {
//                        add = true;
//                    }
//                }
                if (result == null) {
                    // add all the previous ones...
                    result = new HashSet<>(expressions.subList(0, i));
                }
//                if (!Regex.isSimpleMatchPattern(expression)) {
//                    if (!unavailableIgnoredOrExists(options, metaData, expression)) {
//                        throw infe(expression);
//                    }
//                    if (add) {
//                        result.add(expression);
//                    } else {
//                        result.remove(expression);
//                    }
//                    continue;
//                }
//
//                final IndexMetaData.State excludeState = excludeState(options);
//                final Map<String, AliasOrIndex> matches = matches(metaData, expression);
//                Set<String> expand = expand(context, excludeState, matches);
//                if (add) {
//                    result.addAll(expand);
//                } else {
//                    result.removeAll(expand);
//                }
//
//                if (!noIndicesAllowedOrMatches(options, matches)) {
//                    throw infe(expression);
//                }

                if (Regex.isSimpleMatchPattern(expression)) {
                    wildcardSeen = true;
                }
            }

            return new ArrayList<>(result);
        }

        private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
            return expressions.isEmpty() || (expressions.size() == 1 && Regex.isMatchAllPattern(expressions.get(0)));
        }

        private List<String> resolveEmptyOrTrivialWildcard(Map<String, RemoteClusterConnection> remoteClusters) {
            return new ArrayList<>(remoteClusters.keySet());
        }
    }
}
