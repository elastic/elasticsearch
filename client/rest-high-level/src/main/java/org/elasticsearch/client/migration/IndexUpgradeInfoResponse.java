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
package org.elasticsearch.client.migration;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response object that contains information about indices to be upgraded
 */
public class IndexUpgradeInfoResponse {

    private static final ParseField INDICES = new ParseField("indices");
    private static final ParseField ACTION_REQUIRED = new ParseField("action_required");

    private static final ConstructingObjectParser<IndexUpgradeInfoResponse, String> PARSER =
        new ConstructingObjectParser<>("IndexUpgradeInfoResponse",
            true,
            (a, c) -> {
            @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>)a[0];
                Map<String, UpgradeActionRequired> actionsRequired = map.entrySet().stream()
                    .filter(e -> {
                        if (e.getValue() instanceof Map == false) {
                            return false;
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> value =(Map<String, Object>)e.getValue();
                        return value.containsKey(ACTION_REQUIRED.getPreferredName());
                    })
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> value = (Map<String, Object>) e.getValue();
                            return UpgradeActionRequired.fromString((String)value.get(ACTION_REQUIRED.getPreferredName()));
                        }
                ));
                return new IndexUpgradeInfoResponse(actionsRequired);
            });

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), INDICES);
    }


    private final Map<String, UpgradeActionRequired> actions;

    public IndexUpgradeInfoResponse(Map<String, UpgradeActionRequired> actions) {
        this.actions = actions;
    }

    public Map<String, UpgradeActionRequired> getActions() {
        return actions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexUpgradeInfoResponse response = (IndexUpgradeInfoResponse) o;
        return Objects.equals(actions, response.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions);
    }

    public static IndexUpgradeInfoResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
