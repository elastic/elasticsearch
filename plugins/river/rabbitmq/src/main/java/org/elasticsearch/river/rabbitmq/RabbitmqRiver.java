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

package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class RabbitmqRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;

    @Inject public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
    }

    @Override public void start() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(XContentMapValues.nodeStringValue(settings.settings().get("host"), ConnectionFactory.DEFAULT_HOST));
        connectionFactory.setPort(XContentMapValues.nodeIntegerValue(settings.settings().get("port"), ConnectionFactory.DEFAULT_AMQP_PORT));
        connectionFactory.setUsername(XContentMapValues.nodeStringValue(settings.settings().get("user"), ConnectionFactory.DEFAULT_USER));
        connectionFactory.setPassword(XContentMapValues.nodeStringValue(settings.settings().get("password"), ConnectionFactory.DEFAULT_PASS));
        connectionFactory.setVirtualHost(XContentMapValues.nodeStringValue(settings.settings().get("vhost"), ConnectionFactory.DEFAULT_VHOST));

        logger.info("creating rabbitmq river, host [{}], port [{}], user [{}], vhost [{}]", connectionFactory.getHost(), connectionFactory.getPort(), connectionFactory.getUsername(), connectionFactory.getVirtualHost());

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "rabbitmq_river").newThread(new Consumer());
        thread.start();
    }

    @Override public void close() {
        if (closed) {
            return;
        }
        logger.info("closing rabbitmq river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        private Connection connection;

        private Channel channel;

        @Override public void run() {
            while (true) {
                if (closed) {
                    return;
                }
                try {
                    connection = connectionFactory.newConnection();
                    channel = connection.createChannel();
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection / channel", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }

                int bulkSize = XContentMapValues.nodeIntegerValue(settings.settings().get("bulk_size"), 100);
                long bulkTimeout = XContentMapValues.nodeIntegerValue(settings.settings().get("bulk_timeout"), 10);
                String queueName = XContentMapValues.nodeStringValue(settings.settings().get("queue"), "elasticsearch");

                QueueingConsumer consumer = new QueueingConsumer(channel);
                // define the queue
                try {
                    channel.exchangeDeclare(queueName, "direct", true);
                    channel.queueDeclare(queueName, true, false, false, null);
                    channel.queueBind(queueName, queueName, queueName);
                    channel.basicConsume(queueName, false, consumer);
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create queue [{}]", e, queueName);
                    }
                    cleanup(0, "failed to create queue");
                    continue;
                }

                // now use the queue to listen for messages
                while (true) {
                    if (closed) {
                        break;
                    }
                    QueueingConsumer.Delivery task;
                    try {
                        task = consumer.nextDelivery();
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to get next message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }

                    if (task != null && task.getBody() != null) {
                        final List<Long> deliveryTags = Lists.newArrayList();

                        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                        try {
                            bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                        } catch (Exception e) {
                            logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                            try {
                                channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                            } catch (IOException e1) {
                                logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
                            }
                            continue;
                        }

                        deliveryTags.add(task.getEnvelope().getDeliveryTag());

                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                            try {
                                while ((task = consumer.nextDelivery(bulkTimeout)) != null) {
                                    try {
                                        bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                                    } catch (Exception e) {
                                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                                        try {
                                            channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                                        } catch (IOException e1) {
                                            logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
                                        }
                                    }
                                    deliveryTags.add(task.getEnvelope().getDeliveryTag());
                                    if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
                                        break;
                                    }
                                }
                            } catch (InterruptedException e) {
                                if (closed) {
                                    break;
                                }
                            }
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                        }

                        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                            @Override public void onResponse(BulkResponse response) {
                                if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                }
                                for (Long deliveryTag : deliveryTags) {
                                    try {
                                        channel.basicAck(deliveryTag, false);
                                    } catch (IOException e1) {
                                        logger.warn("failed to ack [{}]", e1, deliveryTag);
                                    }
                                }
                            }

                            @Override public void onFailure(Throwable e) {
                                logger.warn("failed to execute bulk for delivery tags , not ack'ing", e, deliveryTags);
                            }
                        });
                    }
                }
            }
        }

        private void cleanup(int code, String message) {
            try {
                channel.close(code, message);
            } catch (Exception e) {
                logger.debug("failed to close channel", e);
            }
            try {
                connection.close(code, message);
            } catch (Exception e) {
                logger.debug("failed to close connection", e);
            }
        }
    }
}
