/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.enterprise.monitor.exporter;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class ESExporter extends AbstractLifecycleComponent<ESExporter> implements StatsExporter<ESExporter> {

    final String[] hosts;
    final String indexPrefix;
    final DateTimeFormatter indexTimeFormatter;
    final int timeout;

    final ClusterName clusterName;

    final ESLogger logger = ESLoggerFactory.getLogger(ESExporter.class.getName());

    final ToXContent.Params xContentParams;

    public final static DateTimeFormatter defaultDatePrinter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    boolean checkedForIndexTemplate = false;


    public ESExporter(Settings settings, ClusterName clusterName) {
        super(settings);

        this.clusterName = clusterName;

        hosts = settings.getAsArray("hosts", new String[]{"localhost:9200"});
        indexPrefix = settings.get("index.prefix", "es_monitor");
        String indexTimeFormat = settings.get("index.timeformat", "YYYY.MM.dd");
        indexTimeFormatter = DateTimeFormat.forPattern(indexTimeFormat);

        timeout = (int) settings.getAsTime("timeout", new TimeValue(6000)).seconds();

        xContentParams = new ToXContent.MapParams(
                ImmutableMap.of("load_average_format", "hash", "routing_format", "full"));


        logger.info("ESExporter initialized. Targets: {}, index prefix [{}], index time format [{}]", hosts, indexPrefix, indexTimeFormat);

    }

    @Override
    public String name() {
        return "ESExporter";
    }


    @Override
    public void exportNodeStats(NodeStats nodeStats) {
        exportXContent("nodestats", new ToXContent[]{nodeStats}, nodeStats.getTimestamp());
    }

    @Override
    public void exportShardStats(ShardStats[] shardStatsArray) {
        exportXContent("shardstats", shardStatsArray, System.currentTimeMillis());
    }

    private void exportXContent(String type, ToXContent[] xContentArray, long collectionTimestamp) {
        if (xContentArray == null || xContentArray.length == 0) {
            return;
        }

        if (!checkedForIndexTemplate) {
            if (!checkForIndexTemplate()) {
                logger.debug("no template defined yet. skipping");
                return;
            }
            ;
        }

        logger.debug("exporting {}", type);
        HttpURLConnection conn = openConnection("POST", "/_bulk", XContentType.SMILE.restContentType());
        if (conn == null) {
            logger.error("could not connect to any configured elasticsearch instances: [{}]", hosts);
            return;
        }
        try {
            OutputStream os = conn.getOutputStream();
            // TODO: find a way to disable builder's substream flushing or something neat solution
            for (ToXContent xContent : xContentArray) {
                XContentBuilder builder = XContentFactory.smileBuilder(os);
                builder.startObject().startObject("index")
                        .field("_index", getIndexName()).field("_type", type).endObject().endObject();
                builder.flush();
                os.write(SmileXContent.smileXContent.streamSeparator());

                builder = XContentFactory.smileBuilder(os);
                builder.humanReadable(false);
                builder.startObject();
                builder.field("@timestamp", defaultDatePrinter.print(collectionTimestamp));
                xContent.toXContent(builder, xContentParams);
                builder.endObject();
                builder.flush();
                os.write(SmileXContent.smileXContent.streamSeparator());

            }
            os.close();

            if (conn.getResponseCode() != 200) {
                logConnectionError("remote target didn't respond with 200 OK", conn);
            } else {
                conn.getInputStream().close(); // close and release to connection pool.
            }


        } catch (IOException e) {
            logger.error("error sending data", e);
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


    private String getIndexName() {
        return indexPrefix + "-" + indexTimeFormatter.print(System.currentTimeMillis());

    }


    private HttpURLConnection openConnection(String method, String uri) {
        return openConnection(method, uri, null);
    }

    private HttpURLConnection openConnection(String method, String uri, String contentType) {
        for (String host : hosts) {
            try {
                URL templateUrl = new URL("http://" + host + uri);
                HttpURLConnection conn = (HttpURLConnection) templateUrl.openConnection();
                conn.setRequestMethod(method);
                conn.setConnectTimeout(timeout);
                if (contentType != null) {
                    conn.setRequestProperty("Content-Type", XContentType.SMILE.restContentType());
                }
                conn.setUseCaches(false);
                if (method.equalsIgnoreCase("POST") || method.equalsIgnoreCase("PUT")) {
                    conn.setDoOutput(true);
                }
                conn.connect();

                return conn;
            } catch (IOException e) {
                logger.error("error connecting to [{}]", e, host);
            }
        }

        return null;
    }

    private boolean checkForIndexTemplate() {
        try {


            String templateName = "enterprise.monitor." + indexPrefix;

            logger.debug("checking of target has template [{}]", templateName);
            // DO HEAD REQUEST, when elasticsearch supports it
            HttpURLConnection conn = openConnection("GET", "/_template/" + templateName);
            if (conn == null) {
                logger.error("Could not connect to any configured elasticsearch instances: [{}]", hosts);
                return false;
            }

            boolean hasTemplate = conn.getResponseCode() == 200;

            // nothing there, lets create it
            if (!hasTemplate) {
                logger.debug("no template found in elasticsearch for [{}]. Adding...", templateName);
                conn = openConnection("PUT", "/_template/" + templateName, XContentType.SMILE.restContentType());
                OutputStream os = conn.getOutputStream();
                XContentBuilder builder = XContentFactory.smileBuilder(os);
                builder.startObject();
                builder.field("template", indexPrefix + "*");
                builder.startObject("mappings").startObject("_default_");
                builder.startArray("dynamic_templates").startObject().startObject("string_fields")
                        .field("match", "*")
                        .field("match_mapping_type", "string")
                        .startObject("mapping").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endArray();
                builder.endObject().endObject(); // mapping + root object.
                builder.close();
                os.close();

                if (conn.getResponseCode() != 200) {
                    logConnectionError("error adding index template to elasticsearch", conn);
                }
                conn.getInputStream().close(); // close and release to connection pool.

            }
            checkedForIndexTemplate = true;
        } catch (IOException e) {
            logger.error("error when checking/adding template to elasticsearch", e);
            return false;
        }
        return true;
    }

    private void logConnectionError(String msg, HttpURLConnection conn) {
        InputStream inputStream = conn.getErrorStream();
        java.util.Scanner s = new java.util.Scanner(inputStream, "UTF-8").useDelimiter("\\A");
        String err = s.hasNext() ? s.next() : "";
        try {
            logger.error("{} response code [{} {}]. content: {}", msg, conn.getResponseCode(), conn.getResponseMessage(), err);
        } catch (IOException e) {
            logger.error("connection had an error while reporting the error. tough life.");
        }
    }
}

