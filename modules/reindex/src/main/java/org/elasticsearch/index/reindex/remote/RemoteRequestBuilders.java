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

package org.elasticsearch.index.reindex.remote;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Builds requests for remote version of Elasticsearch. Note that unlike most of the
 * rest of Elasticsearch this file needs to be compatible with very old versions of
 * Elasticsearch. Thus it often uses identifiers for versions like {@code 2000099}
 * for {@code 2.0.0-alpha1}. Do not drop support for features from this file just
 * because the version constants have been removed.
 */
final class RemoteRequestBuilders {
    private RemoteRequestBuilders() {}

    static Request initialSearch(SearchRequest searchRequest, BytesReference query, Version remoteVersion) {
        // It is nasty to build paths with StringBuilder but we'll be careful....
        StringBuilder path = new StringBuilder("/");
        addIndices(path, searchRequest.indices());
        path.append("_search");
        Request request = new Request("POST", path.toString());

        if (searchRequest.scroll() != null) {
            TimeValue keepAlive = searchRequest.scroll().keepAlive();
            // V_5_0_0
            if (remoteVersion.before(Version.fromId(5000099))) {
                /* Versions of Elasticsearch before 5.0 couldn't parse nanos or micros
                 * so we toss out that resolution, rounding up because more scroll
                 * timeout seems safer than less. */
                keepAlive = timeValueMillis((long) Math.ceil(keepAlive.millisFrac()));
            }
            request.addParameter("scroll", keepAlive.getStringRep());
        }
        request.addParameter("size", Integer.toString(searchRequest.source().size()));

        if (searchRequest.source().version() == null || searchRequest.source().version() == false) {
            request.addParameter("version", Boolean.FALSE.toString());
        } else {
            request.addParameter("version", Boolean.TRUE.toString());
        }

        if (searchRequest.source().sorts() != null) {
            boolean useScan = false;
            // Detect if we should use search_type=scan rather than a sort
            if (remoteVersion.before(Version.fromId(2010099))) {
                for (SortBuilder<?> sort : searchRequest.source().sorts()) {
                    if (sort instanceof FieldSortBuilder) {
                        FieldSortBuilder f = (FieldSortBuilder) sort;
                        if (f.getFieldName().equals(FieldSortBuilder.DOC_FIELD_NAME)) {
                            useScan = true;
                            break;
                        }
                    }
                }
            }
            if (useScan) {
                request.addParameter("search_type", "scan");
            } else {
                StringBuilder sorts = new StringBuilder(sortToUri(searchRequest.source().sorts().get(0)));
                for (int i = 1; i < searchRequest.source().sorts().size(); i++) {
                    sorts.append(',').append(sortToUri(searchRequest.source().sorts().get(i)));
                }
                request.addParameter("sort", sorts.toString());
            }
        }
        if (remoteVersion.before(Version.fromId(2000099))) {
            // Versions before 2.0.0 need prompting to return interesting fields. Note that timestamp isn't available at all....
            searchRequest.source().storedField("_parent").storedField("_routing").storedField("_ttl");
            if (remoteVersion.before(Version.fromId(1000099))) {
                // Versions before 1.0.0 don't support `"_source": true` so we have to ask for the _source in a funny way.
                if (false == searchRequest.source().storedFields().fieldNames().contains("_source")) {
                    searchRequest.source().storedField("_source");
                }
            }
        }
        if (searchRequest.source().storedFields() != null && false == searchRequest.source().storedFields().fieldNames().isEmpty()) {
            StringBuilder fields = new StringBuilder(searchRequest.source().storedFields().fieldNames().get(0));
            for (int i = 1; i < searchRequest.source().storedFields().fieldNames().size(); i++) {
                fields.append(',').append(searchRequest.source().storedFields().fieldNames().get(i));
            }
            // V_5_0_0
            String storedFieldsParamName = remoteVersion.before(Version.fromId(5000099)) ? "fields" : "stored_fields";
            request.addParameter(storedFieldsParamName, fields.toString());
        }

        if (remoteVersion.onOrAfter(Version.fromId(6030099))) {
            // allow_partial_results introduced in 6.3, running remote reindex against earlier versions still silently discards RED shards.
            request.addParameter("allow_partial_search_results", "false");
        }

        // EMPTY is safe here because we're not calling namedObject
        try (XContentBuilder entity = JsonXContent.contentBuilder();
                XContentParser queryParser = XContentHelper
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, query)) {
            entity.startObject();

            entity.field("query"); {
                /* We're intentionally a bit paranoid here - copying the query
                 * as xcontent rather than writing a raw field. We don't want
                 * poorly written queries to escape. Ever. */
                entity.copyCurrentStructure(queryParser);
                XContentParser.Token shouldBeEof = queryParser.nextToken();
                if (shouldBeEof != null) {
                    throw new ElasticsearchException(
                            "query was more than a single object. This first token after the object is [" + shouldBeEof + "]");
                }
            }

            if (searchRequest.source().fetchSource() != null) {
                entity.field("_source", searchRequest.source().fetchSource());
            } else {
                if (remoteVersion.onOrAfter(Version.fromId(1000099))) {
                    // Versions before 1.0 don't support `"_source": true` so we have to ask for the source as a stored field.
                    entity.field("_source", true);
                }
            }

            entity.endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected error building entity", e);
        }
        return request;
    }

    private static void addIndices(StringBuilder path, String[] indices) {
        if (indices == null || indices.length == 0) {
            return;
        }

        path.append(Arrays.stream(indices).map(RemoteRequestBuilders::encodeIndex).collect(Collectors.joining(","))).append('/');
    }

    private static String encodeIndex(String s) {
        try {
            return URLEncoder.encode(s, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String sortToUri(SortBuilder<?> sort) {
        if (sort instanceof FieldSortBuilder) {
            FieldSortBuilder f = (FieldSortBuilder) sort;
            return f.getFieldName() + ":" + f.order();
        }
        throw new IllegalArgumentException("Unsupported sort [" + sort + "]");
    }

    static Request scroll(String scroll, TimeValue keepAlive, Version remoteVersion) {
        Request request = new Request("POST", "/_search/scroll");

        // V_5_0_0
        if (remoteVersion.before(Version.fromId(5000099))) {
            /* Versions of Elasticsearch before 5.0 couldn't parse nanos or micros
             * so we toss out that resolution, rounding up so we shouldn't end up
             * with 0s. */
            keepAlive = timeValueMillis((long) Math.ceil(keepAlive.millisFrac()));
        }
        request.addParameter("scroll", keepAlive.getStringRep());

        if (remoteVersion.before(Version.fromId(2000099))) {
            // Versions before 2.0.0 extract the plain scroll_id from the body
            request.setEntity(new NStringEntity(scroll, ContentType.TEXT_PLAIN));
            return request;
        }

        try (XContentBuilder entity = JsonXContent.contentBuilder()) {
            entity.startObject()
                    .field("scroll_id", scroll)
                .endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build scroll entity", e);
        }
        return request;
    }

    static Request clearScroll(String scroll, Version remoteVersion) {
        Request request = new Request("DELETE", "/_search/scroll");

        if (remoteVersion.before(Version.fromId(2000099))) {
            // Versions before 2.0.0 extract the plain scroll_id from the body
            request.setEntity(new NStringEntity(scroll, ContentType.TEXT_PLAIN));
            return request;
        }
        try (XContentBuilder entity = JsonXContent.contentBuilder()) {
            entity.startObject()
                    .array("scroll_id", scroll)
                .endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build clear scroll entity", e);
        }
        return request;
    }
}
