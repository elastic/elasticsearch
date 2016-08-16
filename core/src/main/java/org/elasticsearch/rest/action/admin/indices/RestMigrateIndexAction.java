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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.migrate.MigrateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Map;

public class RestMigrateIndexAction extends BaseRestHandler {
    private static final ObjectParser<MigrateIndexRequest, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("migrate", null);
    static {
        PARSER.declareObject(MigrateIndexRequest::setScript, (p, c) -> {
            try {
                return Script.parse(p, c.getParseFieldMatcher());
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Error parsing script", e);
            }
        }, new ParseField("script"));
        PARSER.declareObject((v, s) -> v.getCreateIndexRequest().settings(s), (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Error parsing settings", e);
            }
        }, new ParseField("settings"));
        PARSER.declareObject((v, mappings) -> {
            for (Map.Entry<String, Object> mapping: mappings.entrySet()) {
                v.getCreateIndexRequest().mapping(mapping.getKey(), (Map<?, ?>) mapping.getValue());
            }
        }, (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Error parsing mappings", e);
            }
        }, new ParseField("mappings"));
        PARSER.declareObject((v, a) -> v.getCreateIndexRequest().aliases(a), (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Error parsing aliases", e);
            }
        }, new ParseField("aliases"));
    }

    @Inject
    public RestMigrateIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_migrate/{new_index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws IOException {
        MigrateIndexRequest migrate = new MigrateIndexRequest(request.param("index"), request.param("new_index"));
        if (request.hasContent()) {
            try (XContentParser parser = XContentHelper.createParser(request.content())) {
                PARSER.parse(parser, migrate, () -> ParseFieldMatcher.STRICT);
            }
        }
        migrate.timeout(request.paramAsTime("timeout", migrate.timeout()));
        migrate.masterNodeTimeout(request.paramAsTime("master_timeout", migrate.masterNodeTimeout()));
        migrate.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        client.admin().indices().migrateIndex(migrate, new RestToXContentListener<>(channel));
    }

}
