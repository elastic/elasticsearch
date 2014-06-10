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

package org.elasticsearch.rest.action.bench;

import com.google.common.primitives.Doubles;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.bench.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseActionRequestRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.admin.indices.cache.clear.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for submit benchmark action.
 */
public class RestBenchSubmitAction extends BaseActionRequestRestHandler<BenchmarkRequest> {

    @Inject
    public RestBenchSubmitAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(PUT, "/_bench", this);
        controller.registerHandler(PUT, "/{index}/_bench", this);
        controller.registerHandler(PUT, "/{index}/{type}/_bench", this);
    }

    @Override
    protected BenchmarkRequest newRequest(RestRequest request) throws Exception {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        BenchmarkRequestBuilder builder = new BenchmarkRequestBuilder(client);
        builder.setVerbose(request.paramAsBoolean("verbose", false));
        final BenchmarkRequest benchmarkRequest = parse(builder, request.content(), request.contentUnsafe());
        benchmarkRequest.cascadeGlobalSettings();                   // Make sure competitors inherit global settings
        benchmarkRequest.applyLateBoundSettings(indices, types);    // Some settings cannot be applied until after parsing
        benchmarkRequest.listenerThreaded(false);
        return benchmarkRequest;
    }

    @Override
    protected void doHandleRequest(RestRequest restRequest, RestChannel restChannel, BenchmarkRequest request) {
        client.bench(request, new RestToXContentListener<BenchmarkResponse>(restChannel));
    }

    private static BenchmarkRequest parse(BenchmarkRequestBuilder builder, BytesReference data, boolean contentUnsafe) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        XContentParser p = xContent.createParser(data);
        XContentParser.Token token = p.nextToken();
        assert token == XContentParser.Token.START_OBJECT;
        String fieldName = null;
        while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case START_ARRAY:
                    if ("requests".equals(fieldName)) {
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            assert token == XContentParser.Token.START_OBJECT;
                            XContentBuilder payloadBuilder = XContentFactory.contentBuilder(p.contentType()).copyCurrentStructure(p);
                            SearchRequest req = new SearchRequest();
                            req.source(payloadBuilder.bytes(), contentUnsafe);
                            builder.addSearchRequest(req);
                        }
                    } else if ("competitors".equals(fieldName)) {
                        while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                            builder.addCompetitor(parse(p, contentUnsafe));
                        }
                    } else if ("percentiles".equals(fieldName)) {
                        List<Double> percentiles = new ArrayList<>();
                        while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                            percentiles.add(p.doubleValue());
                        }
                        builder.setPercentiles(Doubles.toArray(percentiles));
                    } else {
                        throw new ElasticsearchParseException("Failed parsing array field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case START_OBJECT:
                    if ("clear_caches".equals(fieldName)) {
                        BenchmarkSettings.ClearCachesSettings clearCachesSettings = new BenchmarkSettings.ClearCachesSettings();
                        builder.setClearCachesSettings(clearCachesSettings);
                        parseClearCaches(p, clearCachesSettings);
                    } else {
                        throw new ElasticsearchParseException("Failed parsing object field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case FIELD_NAME:
                    fieldName = p.text();
                    break;
                case VALUE_NUMBER:
                    if ("num_executor_nodes".equals(fieldName)) {
                        builder.setNumExecutorNodes(p.intValue());
                    } else if ("iterations".equals(fieldName)) {
                        builder.setIterations(p.intValue());
                    } else if ("concurrency".equals(fieldName)) {
                        builder.setConcurrency(p.intValue());
                    } else if ("multiplier".equals(fieldName)) {
                        builder.setMultiplier(p.intValue());
                    } else if ("num_slowest".equals(fieldName)) {
                        builder.setNumSlowest(p.intValue());
                    } else {
                        throw new ElasticsearchParseException("Failed parsing numeric field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_BOOLEAN:
                    if ("warmup".equals(fieldName)) {
                        builder.setWarmup(p.booleanValue());
                    } else if ("clear_caches".equals(fieldName)) {
                        if (p.booleanValue()) {
                            throw new ElasticsearchParseException("Failed parsing field [" + fieldName + "] must specify which caches to clear");
                        } else {
                            builder.setAllowCacheClearing(false);
                        }
                    } else {
                        throw new ElasticsearchParseException("Failed parsing boolean field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_STRING:
                    if ("name".equals(fieldName)) {
                        builder.setBenchmarkId(p.text());
                    } else {
                        throw new ElasticsearchParseException("Failed parsing string field [" + fieldName + "] field is not recognized");
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
            }
        }

        return builder.request();
    }

    private static BenchmarkCompetitorBuilder parse(XContentParser p, boolean contentUnsafe) throws Exception {
        XContentParser.Token token = p.currentToken();
        BenchmarkCompetitorBuilder builder = new BenchmarkCompetitorBuilder();
        assert token == XContentParser.Token.START_OBJECT;
        String fieldName = null;
        while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case START_ARRAY:
                    if ("requests".equals(fieldName)) {
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            assert token == XContentParser.Token.START_OBJECT;
                            XContentBuilder payloadBuilder = XContentFactory.contentBuilder(p.contentType()).copyCurrentStructure(p);
                            SearchRequest req = new SearchRequest();
                            req.source(payloadBuilder.bytes(), contentUnsafe);
                            builder.addSearchRequest(req);
                        }
                    } else if ("indices".equals(fieldName)) {
                        List<String> perCompetitorIndices = new ArrayList<>();
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                perCompetitorIndices.add(p.text());
                            } else {
                                throw new ElasticsearchParseException("Failed parsing array field [" + fieldName + "] expected string values but got: " + token);
                            }
                        }
                        builder.setIndices(perCompetitorIndices.toArray(new String[perCompetitorIndices.size()]));
                    } else if ("types".equals(fieldName)) {
                        List<String> perCompetitorTypes = new ArrayList<>();
                        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                perCompetitorTypes.add(p.text());
                            } else {
                                throw new ElasticsearchParseException("Failed parsing array field [" + fieldName + "] expected string values but got: " + token);
                            }
                        }
                        builder.setTypes(perCompetitorTypes.toArray(new String[perCompetitorTypes.size()]));
                    } else {
                        throw new ElasticsearchParseException("Failed parsing array field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case START_OBJECT:
                    if ("clear_caches".equals(fieldName)) {
                        BenchmarkSettings.ClearCachesSettings clearCachesSettings = new BenchmarkSettings.ClearCachesSettings();
                        builder.setClearCachesSettings(clearCachesSettings);
                        parseClearCaches(p, clearCachesSettings);
                    } else {
                        throw new ElasticsearchParseException("Failed parsing object field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case FIELD_NAME:
                    fieldName = p.text();
                    break;
                case VALUE_NUMBER:
                    if ("multiplier".equals(fieldName)) {
                        builder.setMultiplier(p.intValue());
                    } else if ("num_slowest".equals(fieldName)) {
                        builder.setNumSlowest(p.intValue());
                    } else if ("iterations".equals(fieldName)) {
                        builder.setIterations(p.intValue());
                    } else if ("concurrency".equals(fieldName)) {
                        builder.setConcurrency(p.intValue());
                    } else {
                        throw new ElasticsearchParseException("Failed parsing numeric field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_BOOLEAN:
                    if ("warmup".equals(fieldName)) {
                        builder.setWarmup(p.booleanValue());
                    } else if ("clear_caches".equals(fieldName)) {
                        if (p.booleanValue()) {
                            throw new ElasticsearchParseException("Failed parsing field [" + fieldName + "] must specify which caches to clear");
                        } else {
                            builder.setAllowCacheClearing(false);
                        }
                    } else {
                        throw new ElasticsearchParseException("Failed parsing boolean field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case VALUE_STRING:
                    if ("name".equals(fieldName)) {
                        builder.setName(p.text());
                    } else if ("search_type".equals(fieldName) || "searchType".equals(fieldName)) {
                        builder.setSearchType(SearchType.fromString(p.text()));
                    } else {
                        throw new ElasticsearchParseException("Failed parsing string field [" + fieldName + "] field is not recognized");
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
            }
        }
        return builder;
    }

    private static void parseClearCaches(XContentParser p, BenchmarkSettings.ClearCachesSettings clearCachesSettings) throws Exception {
        XContentParser.Token token;
        String fieldName = null;
        while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
                case START_OBJECT:
                    break;
                case VALUE_BOOLEAN:
                    if (RestClearIndicesCacheAction.Fields.FILTER.match(fieldName)) {
                        clearCachesSettings.filterCache(p.booleanValue());
                    } else if (RestClearIndicesCacheAction.Fields.FIELD_DATA.match(fieldName)) {
                        clearCachesSettings.fieldDataCache(p.booleanValue());
                    } else if (RestClearIndicesCacheAction.Fields.ID.match(fieldName)) {
                        clearCachesSettings.idCache(p.booleanValue());
                    } else if (RestClearIndicesCacheAction.Fields.RECYCLER.match(fieldName)) {
                        clearCachesSettings.recycler(p.booleanValue());
                    } else {
                        throw new ElasticsearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case START_ARRAY:
                    List<String> fields = new ArrayList<>();
                    while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(p.text());
                    }
                    if (RestClearIndicesCacheAction.Fields.FIELDS.match(fieldName)) {
                        clearCachesSettings.fields(fields.toArray(new String[fields.size()]));
                    } else if (RestClearIndicesCacheAction.Fields.FILTER_KEYS.match(fieldName)) {
                        clearCachesSettings.filterKeys(fields.toArray(new String[fields.size()]));
                    } else {
                        throw new ElasticsearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
                    }
                    break;
                case FIELD_NAME:
                    fieldName = p.text();
                    break;
                default:
                    throw new ElasticsearchParseException("Failed parsing " + token.name() + " field [" + fieldName + "] field is not recognized");
            }
        }
    }
}
