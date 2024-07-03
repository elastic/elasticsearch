/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Builder for requests to bulk add a roles to the security index
 */
public class BulkPutRoleRequestBuilder extends ActionRequestBuilder<BulkPutRolesRequest, BulkRolesResponse> {

    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder().allowDescription(true).build();
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<List<RoleDescriptor>, Void> PARSER = new ConstructingObjectParser<>(
        "bulk_update_roles_request_payload",
        a -> (List<RoleDescriptor>) a[0]
    );

    static {
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> {
            p.nextToken();
            return ROLE_DESCRIPTOR_PARSER.parse(n, p, false);
        }, new ParseField("roles"));
    }

    public BulkPutRoleRequestBuilder(ElasticsearchClient client) {
        super(client, ActionTypes.BULK_PUT_ROLES, new BulkPutRolesRequest());
    }

    public BulkPutRoleRequestBuilder content(BytesReference content, XContentType xContentType) throws IOException {
        XContentParser parser = XContentHelper.createParserNotCompressed(
            LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
            content,
            xContentType
        );
        List<RoleDescriptor> roles = PARSER.parse(parser, null);
        request.setRoles(roles);
        return this;
    }

    public BulkPutRoleRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        request.setRefreshPolicy(refreshPolicy);
        return this;
    }
}
