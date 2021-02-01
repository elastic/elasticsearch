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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Request to reindex some documents from one index to another. This implements CompositeIndicesRequest but in a misleading way. Rather than
 * returning all the subrequests that it will make it tries to return a representative set of subrequests. This is best-effort for a bunch
 * of reasons, not least of which that scripts are allowed to change the destination request in drastic ways, including changing the index
 * to which documents are written.
 */
public class ReindexRequestV7  {


    static final ObjectParser<ReindexRequest, Void> PARSER = new ObjectParser<>("reindex_v7");

    static {
        ObjectParser.Parser<ReindexRequest, Void> sourceParser = (parser, request, context) -> {
            // Funky hack to work around Search not having a proper ObjectParser and us wanting to extract query if using remote.
            Map<String, Object> source = parser.map();
            String[] indices = ReindexRequest.extractStringArray(source, "index");
            if (indices != null) {
                request.getSearchRequest().indices(indices);
            }
            request.setRemoteInfo(ReindexRequest.buildRemoteInfo(source));
            XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
            builder.map(source);
            try (InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser innerParser = parser.contentType().xContent()
                     .createParser(parser.getXContentRegistry(), parser.getDeprecationHandler(), stream)) {
                request.getSearchRequest().source().parseXContent(innerParser, false);
            }
        };

        ObjectParser<IndexRequest, Void> destParser = new ObjectParser<>("dest");
        destParser.declareString(IndexRequest::index, new ParseField("index"));
        destParser.declareString(IndexRequest::routing, new ParseField("routing"));
        destParser.declareString(IndexRequest::opType, new ParseField("op_type"));
        destParser.declareString(IndexRequest::setPipeline, new ParseField("pipeline"));
        destParser.declareString((s, i) -> s.versionType(VersionType.fromString(i)), new ParseField("version_type"));

        PARSER.declareField(sourceParser::parse, new ParseField("source"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField((p, v, c) -> destParser.parse(p, v.getDestination(), c), new ParseField("dest"), ObjectParser.ValueType.OBJECT);

        PARSER.declareInt(ReindexRequest::setMaxDocsValidateIdentical, new ParseField("max_docs", "size"));

        PARSER.declareField((p, v, c) -> v.setScript(Script.parse(p)), new ParseField("script"),
            ObjectParser.ValueType.OBJECT);
        PARSER.declareString(ReindexRequest::setConflicts, new ParseField("conflicts"));
    }

    public static ReindexRequest fromXContentV7(XContentParser parser) throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        PARSER.parse(parser, reindexRequest, null);
        return reindexRequest;
    }


}
