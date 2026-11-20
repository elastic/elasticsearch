/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.internal.SharedSecrets;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EsqlQueryRequestBuilder extends org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder<
    EsqlQueryRequest,
    EsqlQueryResponse> {

    public static EsqlQueryRequestBuilder newAsyncEsqlQueryRequestBuilder(ElasticsearchClient client) {
        return new EsqlQueryRequestBuilder(client);
    }

    public static EsqlQueryRequestBuilder newSyncEsqlQueryRequestBuilder(ElasticsearchClient client) {
        return new EsqlQueryRequestBuilder(client);
    }

    private EsqlQueryRequestBuilder(ElasticsearchClient client) {
        super(client, EsqlQueryAction.INSTANCE, EsqlQueryRequest.syncEsqlQueryRequest(null));
    }

    @Override
    public EsqlQueryRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public EsqlQueryRequestBuilder columnar(boolean columnar) {
        request.columnar(columnar);
        return this;
    }

    @Override
    public EsqlQueryRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public EsqlQueryRequestBuilder pragmas(QueryPragmas pragmas) {
        request.pragmas(pragmas);
        return this;
    }

    public EsqlQueryRequestBuilder waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        request.waitForCompletionTimeout(waitForCompletionTimeout);
        return this;
    }

    public EsqlQueryRequestBuilder keepAlive(TimeValue keepAlive) {
        request.keepAlive(keepAlive);
        return this;
    }

    public EsqlQueryRequestBuilder keepOnCompletion(boolean keepOnCompletion) {
        request.keepOnCompletion(keepOnCompletion);
        return this;
    }

    @Override
    public EsqlQueryRequestBuilder allowPartialResults(boolean allowPartialResults) {
        request.allowPartialResults(allowPartialResults);
        return this;
    }

    @Override
    public org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> profile(boolean profile) {
        request.profile(profile);
        return this;
    }

    @Override
    public EsqlQueryRequestBuilder projectRouting(String projectRouting) {
        request.set(QuerySettings.PROJECT_ROUTING, projectRouting);
        return this;
    }

    @Override
    public EsqlQueryRequestBuilder executionMetadata(ExecutionMetadata mode) {
        Objects.requireNonNull(mode, "mode");
        // The server rejects a request with both flags set, even to false, so the flag not in use stays null.
        request.includeCCSMetadata(mode == ExecutionMetadata.CCS_ONLY ? Boolean.TRUE : null);
        request.includeExecutionMetadata(mode == ExecutionMetadata.ALWAYS ? Boolean.TRUE : null);
        return this;
    }

    /**
     * Mirrors the REST parsing of {@code settings.<name>} in {@link RequestXContent}: the same lookup and checks, and the value
     * goes through the setting's own JSON reader, so both surfaces accept the same values.
     */
    @Override
    public EsqlQueryRequestBuilder setting(String name, @Nullable Object value) {
        QuerySettingDef<?> def = QuerySettings.lookup(name);
        if (def == null) {
            throw new IllegalArgumentException("Unknown setting [" + name + "]");
        }
        if (def.requestBody() == false) {
            throw new IllegalArgumentException("Setting [" + name + "] is not exposed as a request body parameter");
        }
        QuerySettings.warnIfDeprecated(def);
        setFromJsonValue(def, value);
        return this;
    }

    private <T> void setFromJsonValue(QuerySettingDef<T> def, @Nullable Object value) {
        if (value == null) {
            request.set(def, null);
            return;
        }
        T parsed;
        try (
            XContentBuilder json = JsonXContent.contentBuilder().value(value);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(json))
        ) {
            parser.nextToken();
            parsed = def.readFromJson(parser);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse value for setting [" + def.name() + "]: " + e.getMessage(), e);
        }
        request.set(def, parsed);
    }

    @Override
    public EsqlQueryRequestBuilder params(List<EsqlQueryParam> params) {
        List<QueryParam> queryParams = new ArrayList<>(params.size());
        for (EsqlQueryParam param : params) {
            ParserUtils.ParamClassification classification = switch (param.classification()) {
                case VALUE -> ParserUtils.ParamClassification.VALUE;
                case IDENTIFIER -> ParserUtils.ParamClassification.IDENTIFIER;
                case PATTERN -> ParserUtils.ParamClassification.PATTERN;
            };
            DataType dataType = param.classification() == EsqlQueryParam.ParamClassification.VALUE
                ? DataType.fromJava(param.value())
                : DataType.NULL;
            queryParams.add(new QueryParam(param.name(), param.value(), dataType, classification));
        }
        request.params(new QueryParams(queryParams));
        return this;
    }

    static { // plumb access from x-pack core
        SharedSecrets.setEsqlQueryRequestBuilderAccess(EsqlQueryRequestBuilder::newAsyncEsqlQueryRequestBuilder);
    }
}
