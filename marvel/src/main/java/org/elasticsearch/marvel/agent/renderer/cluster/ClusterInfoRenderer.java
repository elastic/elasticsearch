/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClusterInfoRenderer extends AbstractRenderer<ClusterInfoMarvelDoc> {
    public ClusterInfoRenderer() {
        super(null, false);
    }

    @Override
    protected void doRender(ClusterInfoMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(Fields.CLUSTER_NAME, marvelDoc.getClusterName());
        builder.field(Fields.VERSION, marvelDoc.getVersion());

        builder.startArray(Fields.LICENSES);
        List<License> licenses = marvelDoc.getLicenses();
        if (licenses != null) {
            for (License license : licenses) {
                builder.startObject();
                builder.field(Fields.STATUS, license.status().label());
                builder.field(Fields.UID, license.uid());
                builder.field(Fields.TYPE, license.type());
                builder.dateValueField(Fields.ISSUE_DATE_IN_MILLIS, Fields.ISSUE_DATE, license.issueDate());
                builder.field(Fields.FEATURE, license.feature());
                builder.dateValueField(Fields.EXPIRY_DATE_IN_MILLIS, Fields.EXPIRY_DATE, license.expiryDate());
                builder.field(Fields.MAX_NODES, license.maxNodes());
                builder.field(Fields.ISSUED_TO, license.issuedTo());
                builder.field(Fields.ISSUER, license.issuer());
                builder.field(Fields.HKEY, hash(license, marvelDoc.clusterUUID()));
                builder.endObject();

            }
        }
        builder.endArray();

        builder.startObject(Fields.CLUSTER_STATS);
        ClusterStatsResponse clusterStats = marvelDoc.getClusterStats();
        if (clusterStats != null) {
            clusterStats.toXContent(builder, params);
        }
        builder.endObject();
    }

    public static String hash(License license, String clusterName) {
        return hash(license.status().label(), license.uid(), license.type(), String.valueOf(license.expiryDate()), clusterName);
    }

    public static String hash(String licenseStatus, String licenseUid, String licenseType, String licenseExpiryDate, String clusterUUID) {
        String toHash = licenseStatus + licenseUid + licenseType + licenseExpiryDate + clusterUUID;
        return MessageDigests.toHexString(MessageDigests.sha256().digest(toHash.getBytes(StandardCharsets.UTF_8)));
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_NAME = new XContentBuilderString("cluster_name");
        static final XContentBuilderString LICENSES = new XContentBuilderString("licenses");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString CLUSTER_STATS = new XContentBuilderString("cluster_stats");

        static final XContentBuilderString HKEY = new XContentBuilderString("hkey");

        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString UID = new XContentBuilderString("uid");
        static final XContentBuilderString TYPE = new XContentBuilderString("type");
        static final XContentBuilderString FEATURE = new XContentBuilderString("feature");
        static final XContentBuilderString ISSUE_DATE_IN_MILLIS = new XContentBuilderString("issue_date_in_millis");
        static final XContentBuilderString ISSUE_DATE = new XContentBuilderString("issue_date");
        static final XContentBuilderString EXPIRY_DATE_IN_MILLIS = new XContentBuilderString("expiry_date_in_millis");
        static final XContentBuilderString EXPIRY_DATE = new XContentBuilderString("expiry_date");
        static final XContentBuilderString MAX_NODES = new XContentBuilderString("max_nodes");
        static final XContentBuilderString ISSUED_TO = new XContentBuilderString("issued_to");
        static final XContentBuilderString ISSUER = new XContentBuilderString("issuer");
    }
}
