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

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A request to get cluster level stats.
 */
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    private static final ObjectParser<ClusterStatsRequest, Void> PARSER = new ObjectParser<>("_cluster/stats");
    static {
        // nocommit: there must be existing logic to read lists of strings somewhere right?
        CheckedFunction<XContentParser, List<String>, IOException> listParser = parser -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = parser.nextToken();
            } else {
                throw new XContentParseException(parser.getTokenLocation(), "Failed to parse list:  expecting "
                        + XContentParser.Token.START_ARRAY + " but got " + token);
            }
            ArrayList<String> list = new ArrayList<>();
            for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                list.add(parser.text());
            }
            return list;
        };
        ContextParser<Void, Map<String, List<String>>> indexPatternsParser = (parser, context) -> parser.map(HashMap::new, listParser);
        PARSER.declareObject(ClusterStatsRequest::indexPatterns, indexPatternsParser, new ParseField("index_patterns"));
    }

    /** Parse a {@link ClusterStatsRequest} from some {@link XContent}. */
    public static void fromXContent(XContentParser parser, ClusterStatsRequest request) throws IOException {
        PARSER.parse(parser, request, null);
    }

    /**
     * A map from identifiers on an index pattern to a list of index patterns, e.g.
     * <pre class="prettyprint">
     * {
     *   "logs": [ "logs-*", "filebeat-*" ],
     *   "metrics": [ "metrics-*" ]
     * }
     * </pre>
     */
    private Map<String, List<String>> indexPatterns = Collections.emptyMap();

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            indexPatterns = in.readMap(StreamInput::readString, si -> si.readList(StreamInput::readString));
        }
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeMap(indexPatterns, StreamOutput::writeString, (so, list) -> so.writeCollection(list, StreamOutput::writeString));
        }
    }

    /** Return index patterns to compute stats on for this request. */
    public Map<String, List<String>> indexPatterns() {
        return indexPatterns;
    }

    /** Set index patterns to compute stats on. */
    public ClusterStatsRequest indexPatterns(Map<String, List<String>> indexPatterns) {
        this.indexPatterns = Objects.requireNonNull(indexPatterns);
        return this;
    }

}
