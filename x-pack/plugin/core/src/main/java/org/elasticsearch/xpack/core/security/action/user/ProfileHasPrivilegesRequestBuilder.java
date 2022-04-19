/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;

public class ProfileHasPrivilegesRequestBuilder extends ActionRequestBuilder<ProfileHasPrivilegesRequest, ProfileHasPrivilegesResponse> {

    public ProfileHasPrivilegesRequestBuilder(ElasticsearchClient client) {
        super(client, ProfileHasPrivilegesAction.INSTANCE, new ProfileHasPrivilegesRequest());
    }

    public ProfileHasPrivilegesRequestBuilder parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse request for has privileges check. expected an object but found [{}] instead",
                token
            );
        }
        String currentFieldName = null;
        String[] uids = null;
        RoleDescriptor roleDescriptor = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.UIDS.match(currentFieldName, parser.getDeprecationHandler())) {
                uids = XContentUtils.readStringArray(parser, false);
            } else if (Fields.PRIVILEGES.match(currentFieldName, parser.getDeprecationHandler())) {
                roleDescriptor = RoleDescriptor.parsePrivilegesCheck("/profile/has_privileges", parser);
            } else {
                throw new ElasticsearchParseException(
                    "failed to parse request for has privileges check. unexpected field [{}]",
                    currentFieldName
                );
            }
        }
        if (uids == null) {
            throw new ElasticsearchParseException(
                "failed to parse request for has privileges check. [{}] field missing but required",
                Fields.UIDS.getPreferredName()
            );
        }
        if (roleDescriptor == null) {
            throw new ElasticsearchParseException(
                "failed to parse request for has privileges check. [{}] field missing but required",
                Fields.PRIVILEGES.getPreferredName()
            );
        }
        request.profileUids(uids);
        request.clusterPrivileges(roleDescriptor.getClusterPrivileges());
        request.indexPrivileges(roleDescriptor.getIndicesPrivileges());
        request.applicationPrivileges(roleDescriptor.getApplicationPrivileges());
        return this;
    }

    public interface Fields {
        ParseField UIDS = new ParseField("uids");
        ParseField PRIVILEGES = new ParseField("privileges");
    }
}
