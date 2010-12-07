/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.twitter;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import twitter4j.*;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (shay.banon)
 */
public class TwitterRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String indexName;

    private final String typeName;

    private final int bulkSize;

    private final int dropThreshold;

    private FilterQuery filterQuery;

    private String streamType;


    private final TwitterStream stream;

    private final AtomicInteger onGoingBulks = new AtomicInteger();

    private volatile BulkRequestBuilder currentRequest;

    @SuppressWarnings({"unchecked"})
    @Inject public TwitterRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        String user = null;
        String password = null;
        if (settings.settings().containsKey("twitter")) {
            Map<String, Object> twitterSettings = (Map<String, Object>) settings.settings().get("twitter");
            user = XContentMapValues.nodeStringValue(twitterSettings.get("user"), null);
            password = XContentMapValues.nodeStringValue(twitterSettings.get("password"), null);
            streamType = XContentMapValues.nodeStringValue(twitterSettings.get("type"), "sample");
            Map<String, Object> filterSettings = (Map<String, Object>) twitterSettings.get("filter");
            if (filterSettings != null) {
                filterQuery = new FilterQuery();
                filterQuery.count(XContentMapValues.nodeIntegerValue(filterSettings.get("count"), 0));
                Object tracks = filterSettings.get("tracks");
                if (tracks != null) {
                    if (tracks instanceof List) {
                        List<String> lTracks = (List<String>) tracks;
                        filterQuery.track(lTracks.toArray(new String[lTracks.size()]));
                    } else {
                        filterQuery.track(Strings.commaDelimitedListToStringArray(tracks.toString()));
                    }
                }
                Object follow = filterSettings.get("follow");
                if (follow != null) {
                    if (follow instanceof List) {
                        List lFollow = (List) follow;
                        int[] followIds = new int[lFollow.size()];
                        for (int i = 0; i < lFollow.size(); i++) {
                            Object o = lFollow.get(i);
                            if (o instanceof Number) {
                                followIds[i] = ((Number) o).intValue();
                            } else {
                                followIds[i] = Integer.parseInt(o.toString());
                            }
                        }
                        filterQuery.follow(followIds);
                    } else {
                        String[] ids = Strings.commaDelimitedListToStringArray(follow.toString());
                        int[] followIds = new int[ids.length];
                        for (int i = 0; i < ids.length; i++) {
                            followIds[i] = Integer.parseInt(ids[i]);
                        }
                    }
                }
                Object locations = filterSettings.get("locations");
                if (locations != null) {
                    if (locations instanceof List) {
                        List lLocations = (List) locations;
                        double[][] dLocations = new double[lLocations.size()][];
                        for (int i = 0; i < lLocations.size(); i++) {
                            Object loc = lLocations.get(i);
                            double lat;
                            double lon;
                            if (loc instanceof List) {
                                List lLoc = (List) loc;
                                if (lLoc.get(0) instanceof Number) {
                                    lat = ((Number) lLoc.get(0)).doubleValue();
                                } else {
                                    lat = Double.parseDouble(lLoc.get(0).toString());
                                }
                                if (lLoc.get(1) instanceof Number) {
                                    lon = ((Number) lLoc.get(1)).doubleValue();
                                } else {
                                    lon = Double.parseDouble(lLoc.get(1).toString());
                                }
                            } else {
                                String[] sLoc = Strings.commaDelimitedListToStringArray(loc.toString());
                                lat = Double.parseDouble(sLoc[0]);
                                lon = Double.parseDouble(sLoc[1]);
                            }
                            dLocations[i] = new double[]{lat, lon};
                        }
                        filterQuery.locations(dLocations);
                    } else {
                        String[] sLocations = Strings.commaDelimitedListToStringArray(locations.toString());
                        double[][] dLocations = new double[sLocations.length / 2][];
                        int dCounter = 0;
                        for (int i = 0; i < sLocations.length; i++) {
                            double lat = Double.parseDouble(sLocations[i]);
                            double lon = Double.parseDouble(sLocations[++i]);
                            dLocations[dCounter++] = new double[]{lat, lon};
                        }
                        filterQuery.locations(dLocations);
                    }
                }
            }
        }

        logger.info("creating twitter stream river for [{}]", user);

        if (user == null || password == null) {
            stream = null;
            indexName = null;
            typeName = "status";
            bulkSize = 100;
            dropThreshold = 10;
            logger.warn("no user / password specified, disabling river...");
            return;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "status");
            this.bulkSize = XContentMapValues.nodeIntegerValue(settings.settings().get("bulk_size"), 100);
            this.dropThreshold = XContentMapValues.nodeIntegerValue(settings.settings().get("drop_threshold"), 10);
        } else {
            indexName = riverName.name();
            typeName = "status";
            bulkSize = 100;
            dropThreshold = 10;
        }

        stream = new TwitterStreamFactory(new StatusHandler()).getInstance(user, password);
    }

    @Override public void start() {
        if (stream == null) {
            return;
        }
        logger.info("starting twitter stream");
        try {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject(typeName)
                    .startObject("properties").startObject("location").field("type", "geo_point").endObject().endObject()
                    .endObject().endObject().string();
            client.admin().indices().prepareCreate(indexName).addMapping(typeName, mapping).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }
        currentRequest = client.prepareBulk();
        if (streamType.equals("filter") || filterQuery != null) {
            try {
                stream.filter(filterQuery);
            } catch (TwitterException e) {
                logger.warn("failed to create filter stream based on query, disabling river....");
            }
        } else if (streamType.equals("firehose")) {
            stream.firehose(0);
        } else {
            stream.sample();
        }
    }

    @Override public void close() {
        logger.info("closing twitter stream river");
        if (stream != null) {
            stream.cleanUp();
            stream.shutdown();
        }
    }

    private class StatusHandler extends StatusAdapter {

        @Override public void onStatus(Status status) {
            if (logger.isTraceEnabled()) {
                logger.trace("status {} : {}", status.getUser().getName(), status.getText());
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                builder.field("text", status.getText());
                builder.field("created_at", status.getCreatedAt());
                builder.field("source", status.getSource());
                builder.field("truncated", status.isTruncated());


                if (status.getUserMentions() != null) {
                    builder.startArray("mention");
                    for (User user : status.getUserMentions()) {
                        builder.startObject();
                        builder.field("id", user.getId());
                        builder.field("name", user.getName());
                        builder.field("screen_name", user.getScreenName());
                        builder.endObject();
                    }
                    builder.endArray();
                }

                if (status.getRetweetCount() != -1) {
                    builder.field("retweet_count", status.getRetweetCount());
                }

                if (status.getInReplyToStatusId() != -1) {
                    builder.startObject("in_reply");
                    builder.field("status", status.getInReplyToStatusId());
                    if (status.getInReplyToUserId() != -1) {
                        builder.field("user_id", status.getInReplyToUserId());
                        builder.field("user_screen_name", status.getInReplyToScreenName());
                    }
                    builder.endObject();
                }

                if (status.getHashtags() != null) {
                    builder.array("hashtag", status.getHashtags());
                }
                if (status.getContributors() != null) {
                    builder.array("contributor", status.getContributors());
                }
                if (status.getGeoLocation() != null) {
                    builder.startObject("location");
                    builder.field("lat", status.getGeoLocation().getLatitude());
                    builder.field("lon", status.getGeoLocation().getLongitude());
                    builder.endObject();
                }
                if (status.getPlace() != null) {
                    builder.startObject("place");
                    builder.field("id", status.getPlace().getId());
                    builder.field("name", status.getPlace().getName());
                    builder.field("type", status.getPlace().getPlaceType());
                    builder.field("full_name", status.getPlace().getFullName());
                    builder.field("street_address", status.getPlace().getStreetAddress());
                    builder.field("country", status.getPlace().getCountry());
                    builder.field("country_code", status.getPlace().getCountryCode());
                    builder.field("url", status.getPlace().getURL());
                    builder.endObject();
                }
                if (status.getURLs() != null) {
                    builder.startArray("link");
                    for (URL url : status.getURLs()) {
                        if (url != null) {
                            builder.value(url.toExternalForm());
                        }
                    }
                    builder.endArray();
                }
                if (status.getAnnotations() != null) {
                    builder.startObject("annotation");
                    for (Annotation ann : status.getAnnotations().getAnnotations()) {
                        builder.startObject(ann.getType());
                        for (Map.Entry<String, String> entry : ann.getAttributes().entrySet()) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }

                builder.startObject("user");
                builder.field("id", status.getUser().getId());
                builder.field("name", status.getUser().getName());
                builder.field("screen_name", status.getUser().getScreenName());
                builder.field("location", status.getUser().getLocation());
                builder.field("description", status.getUser().getDescription());
                builder.endObject();

                builder.endObject();
                currentRequest.add(Requests.indexRequest(indexName).type(typeName).id(Long.toString(status.getId())).create(true).source(builder));
                processBulkIfNeeded();
            } catch (Exception e) {
                logger.warn("failed to construct index request", e);
            }
        }

        @Override public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            if (statusDeletionNotice.getStatusId() != -1) {
                currentRequest.add(Requests.deleteRequest(indexName).type(typeName).id(Long.toString(statusDeletionNotice.getStatusId())));
                processBulkIfNeeded();
            }
        }

        @Override public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        @Override public void onException(Exception ex) {
            logger.warn("stream failure", ex);
        }

        private void processBulkIfNeeded() {
            if (currentRequest.numberOfActions() >= bulkSize) {
                // execute the bulk operation
                int currentOnGoingBulks = onGoingBulks.incrementAndGet();
                if (currentOnGoingBulks > dropThreshold) {
                    onGoingBulks.decrementAndGet();
                    logger.warn("dropping bulk, [{}] crossed threshold [{}]", onGoingBulks, dropThreshold);
                } else {
                    try {
                        currentRequest.execute(new ActionListener<BulkResponse>() {
                            @Override public void onResponse(BulkResponse bulkResponse) {
                                onGoingBulks.decrementAndGet();
                            }

                            @Override public void onFailure(Throwable e) {
                                logger.warn("failed to execute bulk");
                            }
                        });
                    } catch (Exception e) {
                        logger.warn("failed to process bulk", e);
                    }
                }
                currentRequest = client.prepareBulk();
            }
        }
    }
}
