/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to invalidate one or more API keys
 */
public final class RestInvalidateApiKeyAction extends ApiKeyBaseRestHandler {
    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<InvalidateApiKeyRequest, Void> PARSER = new ConstructingObjectParser<>(
        "invalidate_api_key",
        a -> {
            return new InvalidateApiKeyRequest(
                (String) a[0],
                (String) a[1],
                (String) a[2],
                (a[3] == null) ? false : (Boolean) a[3],
                (a[4] == null) ? null : ((List<String>) a[4]).toArray(new String[0])
            );
        }
    );

    static {
        initObjectParser(PARSER, false);
    }

    public RestInvalidateApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/api_key"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final InvalidateApiKeyRequest invalidateApiKeyRequest = getObjectParser(request).parse(parser, null);
            return channel -> client.execute(
                InvalidateApiKeyAction.INSTANCE,
                invalidateApiKeyRequest,
                new RestBuilderListener<InvalidateApiKeyResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(InvalidateApiKeyResponse invalidateResp, XContentBuilder builder) throws Exception {
                        invalidateResp.toXContent(builder, channel.request());
                        return new RestResponse(RestStatus.OK, builder);
                    }
                }
            );
        }
    }

    @Override
    public String getName() {
        return "xpack_security_invalidate_api_key";
    }

    private static ConstructingObjectParser<InvalidateApiKeyRequest, Void> getObjectParser(RestRequest request) {
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            final ConstructingObjectParser<InvalidateApiKeyRequest, Void> objectParser = new ConstructingObjectParser<>(
                "invalidate_api_key_v7",
                a -> {
                    final String id = (String) a[5];
                    @SuppressWarnings("unchecked")
                    final List<String> ids = (List<String>) a[4];
                    if (id != null && ids != null) {
                        throw new IllegalArgumentException("Must use either [id] or [ids], not both at the same time");
                    }
                    final String[] idsArray;
                    if (Strings.hasText(id)) {
                        idsArray = new String[] { id };
                    } else if (ids != null) {
                        idsArray = ids.toArray(String[]::new);
                    } else {
                        idsArray = null;
                    }
                    return new InvalidateApiKeyRequest(
                        (String) a[0],
                        (String) a[1],
                        (String) a[2],
                        (a[3] == null) ? false : (Boolean) a[3],
                        idsArray
                    );
                }
            );
            initObjectParser(objectParser, true);
            return objectParser;
        } else {
            return PARSER;
        }
    }

    private static void initObjectParser(ConstructingObjectParser<InvalidateApiKeyRequest, Void> objectParser, boolean restCompatMode) {
        objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("realm_name"));
        objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("username"));
        objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("name"));
        objectParser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), new ParseField("owner"));
        objectParser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField("ids"));
        if (restCompatMode) {
            objectParser.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("id").withAllDeprecated("ids"));
        }
    }
}
