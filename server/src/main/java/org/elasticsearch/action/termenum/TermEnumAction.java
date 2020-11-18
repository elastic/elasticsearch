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

package org.elasticsearch.action.termenum;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class TermEnumAction extends ActionType<TermEnumResponse> {

    public static final TermEnumAction INSTANCE = new TermEnumAction();
    public static final String NAME = "indices:admin/termsenum/list";

    private TermEnumAction() {
        super(NAME, TermEnumResponse::new);
    }

    public static TermEnumRequest fromXContent(XContentParser parser, String... indices) throws IOException {
        TermEnumRequest request = new TermEnumRequest(indices);
        PARSER.parse(parser, request, null);
        return request;
    }

    private static final ObjectParser<TermEnumRequest, Void> PARSER = new ObjectParser<>("terms_enum_request");
    static {
        PARSER.declareString(TermEnumRequest::field, new ParseField("field"));
        PARSER.declareString(TermEnumRequest::pattern, new ParseField("pattern"));
        PARSER.declareInt(TermEnumRequest::size, new ParseField("size"));
        PARSER.declareBoolean(TermEnumRequest::leadingWildcard, new ParseField("leading_wildcard"));
        PARSER.declareBoolean(TermEnumRequest::traillingWildcard, new ParseField("trailling_wildcard"));
        PARSER.declareBoolean(TermEnumRequest::caseInsensitive, new ParseField("case_insensitive"));
        PARSER.declareBoolean(TermEnumRequest::useRegexpSyntax, new ParseField("use_regexp_syntax"));
        PARSER.declareBoolean(TermEnumRequest::sortByPopularity, new ParseField("sort_by_popularity"));
        PARSER.declareInt(TermEnumRequest::minShardDocFreq, new ParseField("min_shard_doc_freq"));
        PARSER.declareInt(TermEnumRequest::timeout, new ParseField("timeout"));
    }
}
