/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.ShieldBuild;
import org.elasticsearch.shield.Shield;
import org.elasticsearch.shield.license.ShieldLicenseState;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestShieldInfoAction extends BaseRestHandler {

    private final ClusterName clusterName;
    private final ShieldLicenseState shieldLicenseState;
    private final boolean shieldEnabled;

    @Inject
    public RestShieldInfoAction(Settings settings, RestController controller, Client client, ClusterName clusterName,
                                @Nullable ShieldLicenseState licenseState) {
        super(settings, client);
        this.clusterName = clusterName;
        this.shieldLicenseState = licenseState;
        this.shieldEnabled = Shield.enabled(settings);
        controller.registerHandler(GET, "/_shield", this);
        controller.registerHandler(HEAD, "/_shield", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        if (request.method() == RestRequest.Method.HEAD) {
            channel.sendResponse(new BytesRestResponse(RestStatus.OK));
            return;
        }

        XContentBuilder builder = channel.newBuilder();

        // Default to pretty printing, but allow ?pretty=false to disable
        if (!request.hasParam("pretty")) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.startObject();

        builder.field("status", resolveStatus());
        if (settings.get("name") != null) {
            builder.field("name", settings.get("name"));
        }
        builder.field("cluster_name", clusterName.value());
        builder.startObject("version")
                .field("number", Version.CURRENT.number())
                .field("build_hash", ShieldBuild.CURRENT.hash())
                .field("build_timestamp", ShieldBuild.CURRENT.timestamp())
                .field("build_snapshot", Version.CURRENT.snapshot)
                .endObject();
        builder.field("tagline", "You Know, for Security");
        builder.endObject();

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }

    private Status resolveStatus() {
        if (shieldEnabled) {
            assert shieldLicenseState != null;
            // TODO this is error prone since the state could change between checks. We can also make this status better
            // but we may remove this endpoint since it no longer serves much purpose
            if (shieldLicenseState.securityEnabled() && shieldLicenseState.statsAndHealthEnabled()) {
                return Status.ENABLED;
            }
            return Status.UNLICENSED;
        }
        return Status.DISABLED;
    }

    private static enum Status {
        ENABLED("enabled"), DISABLED("disabled"), UNLICENSED("unlicensed");

        private final String status;

        Status(String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return status;
        }
    }
}
