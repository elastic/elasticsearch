/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package com.elasticsearch.dash.exporters;
import com.elasticsearch.dash.Exporter;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;

public class ESExporter extends AbstractLifecycleComponent<ESExporter> implements Exporter<ESExporter> {

    final String targetHost;
    final int targetPort;

    final String targetPathPrefix;
    final ClusterName clusterName;

    final ESLogger logger = ESLoggerFactory.getLogger(ESExporter.class.getName());

    public ESExporter(Settings settings, ClusterName clusterName) {
        super(settings);

        this.clusterName = clusterName;

        // TODO: move to a single settings.
        targetHost = settings.get("target.host", "localhost");
        targetPort = settings.getAsInt("target.post", 9200);
        String targetIndexPrefix = settings.get("target.index.prefix", "dash");

        try {
            targetPathPrefix = String.format("/%s_%s_",
                    URLEncoder.encode(targetIndexPrefix,"UTF-8"),
                    URLEncoder.encode(clusterName.value(),"UTF-8"));

        } catch (UnsupportedEncodingException e) {
            throw new ElasticSearchException("Can't encode target url", e);
        }


        logger.info("ESExporter initialized. Target: {}:{} Index prefix set to {}", targetHost, targetPort, targetIndexPrefix );
        // explode early on broken settings
        getTargetURL("test");

    }

    private URL getTargetURL(String type) {
        try {
            String path = String.format("%1$s%2$tY.%2$tm.%2$td/%3$s", targetPathPrefix, new Date(), type);
            return new URL("http", targetHost, targetPort, path);
        } catch (MalformedURLException e) {
            throw new ElasticSearchIllegalArgumentException("Target settings result in a malformed url");
        }
    }

    @Override
    public String name() {
        return "ESExporter";
    }

    @Override
    public void exportNodeStats(NodeStats nodeStats) {
        URL url = getTargetURL("nodestats");
        logger.debug("Exporting node stats to {}", url);
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", XContentType.SMILE.restContentType());
            OutputStream os = conn.getOutputStream();
            XContentBuilder builder = XContentFactory.smileBuilder(os);

            builder.startObject();
            nodeStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            builder.close();

            if (conn.getResponseCode() != 201) {
                logger.error("Remote target didn't respond with 201 Created");
            }

        } catch (IOException e) {
            logger.error("Error connecting to target", e);
            return;
        }


    }


    @Override
    protected void doStart() throws ElasticSearchException {
    }


    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

}
