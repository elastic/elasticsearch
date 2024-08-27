/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

/**
 * This package contains infrastructure to collect and provide information necessary on scaling the serverless
 * search tier. Cluster scaling decisions are ultimately made by the
 * <a href="https://github.com/elastic/elasticsearch-autoscaler/">elasticsearch-autoscaler</a>
 * in the control plane, but the services in this package collect and prepare the signals the controllers decisions
 * are based on.
 * <p>
 * There are two main services that perform periodic tasks:
 *
 *  <ul>
 *     <li>{@link co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService}</li>
 *     <li>{@link co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService}</li>
 * </ul>
 *
 * For more information see the service classes javadocs.
 */
package co.elastic.elasticsearch.stateless.autoscaling.search;
