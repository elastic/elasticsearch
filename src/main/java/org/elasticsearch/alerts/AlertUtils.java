/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public final class AlertUtils {

    private AlertUtils() {
    }

    /**
     * Reads a new search request instance for the specified parser.
     */
    public static SearchRequest readSearchRequest(XContentParser parser) throws IOException {
        String searchRequestFieldName = null;
        XContentParser.Token token;
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen()); // TODO: make options configurable
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                searchRequestFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                switch (searchRequestFieldName) {
                    case "indices":
                        List<String> indices = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                indices.add(parser.textOrNull());
                            } else {
                                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
                            }
                        }
                        searchRequest.indices(indices.toArray(new String[indices.size()]));
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + searchRequestFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                XContentBuilder builder;
                switch (searchRequestFieldName) {
                    case "body":
                        builder = XContentBuilder.builder(parser.contentType().xContent());
                        builder.copyCurrentStructure(parser);
                        searchRequest.source(builder);
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + searchRequestFieldName + "]");
                }
            } else if (token.isValue()) {
                switch (searchRequestFieldName) {
                    case "template_name":
                        searchRequest.templateName(parser.textOrNull());
                        break;
                    case "template_type":
                        searchRequest.templateType(readScriptType(parser.textOrNull()));
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + searchRequestFieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected field [" + searchRequestFieldName + "]");
            }
        }
        return searchRequest;
    }

    /**
     * Writes the searchRequest to the specified builder.
     */
    public static void writeSearchRequest(SearchRequest searchRequest, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (Strings.hasLength(searchRequest.source())) {
            XContentHelper.writeRawField("body", searchRequest.source(), builder, params);
        }
        if (searchRequest.templateName() != null) {
            builder.field("template_name", searchRequest.templateName());
        }
        if (searchRequest.templateType() != null) {
            builder.field("template_type", writeScriptType(searchRequest.templateType()));
        }
        builder.startArray("indices");
        for (String index : searchRequest.indices()) {
            builder.value(index);
        }
        builder.endArray();
        builder.endObject();
    }

    private static ScriptService.ScriptType readScriptType(String value) {
        switch (value) {
            case "indexed":
                return ScriptService.ScriptType.INDEXED;
            case "inline":
                return ScriptService.ScriptType.INLINE;
            case "file":
                return ScriptService.ScriptType.FILE;
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown script_type value [" + value + "]");
        }
    }

    private static String writeScriptType(ScriptService.ScriptType value) {
        switch (value) {
            case INDEXED:
                return "indexed";
            case INLINE:
                return "inline";
            case FILE:
                return "file";
            default:
                throw new ElasticsearchIllegalArgumentException("Illegal script_type value [" + value + "]");
        }
    }

}
