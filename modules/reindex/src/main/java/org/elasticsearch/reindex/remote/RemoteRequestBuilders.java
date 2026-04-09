/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

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
            TimeValue keepAlive = searchRequest.scroll();
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
                    if (sort instanceof FieldSortBuilder f) {
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
        try (
            XContentBuilder entity = JsonXContent.contentBuilder();
            XContentParser queryParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, query)
        ) {
            entity.startObject();

            entity.field("query");
            {
                /* We're intentionally a bit paranoid here - copying the query
                 * as xcontent rather than writing a raw field. We don't want
                 * poorly written queries to escape. Ever. */
                entity.copyCurrentStructure(queryParser);
                XContentParser.Token shouldBeEof = queryParser.nextToken();
                if (shouldBeEof != null) {
                    throw new ElasticsearchException(
                        "query was more than a single object. This first token after the object is [" + shouldBeEof + "]"
                    );
                }
            }

            var fetchSource = searchRequest.source().fetchSource();
            if (fetchSource == null) {
                if (remoteVersion.onOrAfter(Version.fromId(1000099))) {
                    // Versions before 1.0 don't support `"_source": true` so we have to ask for the source as a stored field.
                    entity.field("_source", true);
                }
            } else {
                if (remoteVersion.onOrAfter(Version.V_9_1_0) || fetchSource.excludeVectors() == null) {
                    entity.field("_source", fetchSource);
                } else {
                    // Versions before 9.1.0 don't support "exclude_vectors" so we need to manually convert.
                    if (fetchSource.includes().length == 0 && fetchSource.excludes().length == 0) {
                        if (remoteVersion.onOrAfter(Version.fromId(1000099))) {
                            // Versions before 1.0 don't support `"_source": true` so we have to ask for the source as a stored field.
                            entity.field("_source", true);
                        }
                    } else {
                        entity.startObject("_source");
                        if (fetchSource.includes().length > 0) {
                            entity.field(FetchSourceContext.INCLUDES_FIELD.getPreferredName(), fetchSource.includes());
                        }
                        if (fetchSource.excludes().length > 0) {
                            entity.field(FetchSourceContext.EXCLUDES_FIELD.getPreferredName(), fetchSource.excludes());
                        }
                        entity.endObject();
                    }
                }
            }

            if (searchRequest.getProjectRouting() != null) {
                entity.field("project_routing", searchRequest.getProjectRouting());
            }

            entity.endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected error building entity", e);
        }
        return request;
    }

    // TODO - Do we need to set the IndexFilter field here? https://github.com/elastic/elasticsearch-team/issues/2392
    static Request openPit(String[] indices, TimeValue keepAlive, @Nullable String projectRouting) {
        StringBuilder path = new StringBuilder("/");
        addIndices(path, indices);
        path.append("_pit");
        Request request = new Request("POST", path.toString());
        request.addParameter("keep_alive", keepAlive.getStringRep());
        request.addParameter("allow_partial_search_results", "false");
        if (projectRouting != null) {
            try (XContentBuilder entity = JsonXContent.contentBuilder()) {
                entity.startObject().field("project_routing", projectRouting).endObject();
                request.setJsonEntity(Strings.toString(entity));
            } catch (IOException e) {
                throw new ElasticsearchException("unexpected error building open pit entity", e);
            }
        }
        return request;
    }

    static Request closePit(BytesReference pitId) {
        Request request = new Request("DELETE", "/_pit");
        try (XContentBuilder entity = JsonXContent.contentBuilder()) {
            entity.startObject().field("id", java.util.Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pitId))).endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build close pit entity", e);
        }
        return request;
    }

    /**
     * Builds a PIT search request. Requires remote version 7.10.0 or later.
     * Uses POST /_search with pit and optional search_after in the body.
     */
    static Request pitSearch(
        SearchRequest searchRequest,
        BytesReference query,
        BytesReference pitId,
        TimeValue keepAlive,
        @Nullable Object[] searchAfter,
        Version remoteVersion
    ) {
        if (remoteVersion.before(Version.V_7_10_0)) {
            throw new IllegalArgumentException("PIT search requires remote version 7.10.0 or later, but got " + remoteVersion);
        }
        Request request = new Request("POST", "/_search");

        if (remoteVersion.onOrAfter(Version.fromId(6030099))) {
            request.addParameter("allow_partial_search_results", "false");
        }

        try (
            XContentBuilder entity = JsonXContent.contentBuilder();
            XContentParser queryParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, query)
        ) {
            entity.startObject();

            entity.startObject("pit");
            entity.field("id", java.util.Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pitId)));
            entity.field("keep_alive", keepAlive.getStringRep());
            entity.endObject();

            entity.field("size", searchRequest.source().size());

            if (searchRequest.source().version() == null || searchRequest.source().version() == false) {
                entity.field("version", false);
            } else {
                entity.field("version", true);
            }

            List<SortBuilder<?>> sorts = searchRequest.source().sorts();
            if (sorts != null && sorts.isEmpty() == false) {
                entity.startArray("sort");
                for (SortBuilder<?> sort : sorts) {
                    sort.toXContent(entity, ToXContent.EMPTY_PARAMS);
                }
                entity.endArray();
            }

            entity.field("query");
            entity.copyCurrentStructure(queryParser);
            if (queryParser.nextToken() != null) {
                throw new ElasticsearchException("query was more than a single object");
            }

            var fetchSource = searchRequest.source().fetchSource();
            if (fetchSource == null) {
                entity.field("_source", true);
            } else {
                if (remoteVersion.onOrAfter(Version.V_9_1_0) || fetchSource.excludeVectors() == null) {
                    entity.field("_source", fetchSource);
                } else {
                    if (fetchSource.includes().length == 0 && fetchSource.excludes().length == 0) {
                        entity.field("_source", true);
                    } else {
                        entity.startObject("_source");
                        if (fetchSource.includes().length > 0) {
                            entity.field(FetchSourceContext.INCLUDES_FIELD.getPreferredName(), fetchSource.includes());
                        }
                        if (fetchSource.excludes().length > 0) {
                            entity.field(FetchSourceContext.EXCLUDES_FIELD.getPreferredName(), fetchSource.excludes());
                        }
                        entity.endObject();
                    }
                }
            }

            if (searchAfter != null && searchAfter.length > 0) {
                entity.array("search_after", searchAfter);
            }

            entity.endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected error building pit search entity", e);
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
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String sortToUri(SortBuilder<?> sort) {
        if (sort instanceof FieldSortBuilder f) {
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
            entity.startObject().field("scroll_id", scroll).endObject();
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
            entity.startObject().array("scroll_id", scroll).endObject();
            request.setJsonEntity(Strings.toString(entity));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build clear scroll entity", e);
        }
        return request;
    }
}
