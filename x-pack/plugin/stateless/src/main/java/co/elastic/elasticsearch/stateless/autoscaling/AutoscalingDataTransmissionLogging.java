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

package co.elastic.elasticsearch.stateless.autoscaling;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.logging.Level;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;

public class AutoscalingDataTransmissionLogging {

    private AutoscalingDataTransmissionLogging() {}

    public static Level getExceptionLogLevel(Exception exception) {
        return ExceptionsHelper.unwrap(
            exception,
            NodeClosedException.class,
            ConnectTransportException.class,
            MasterNotDiscoveredException.class
        ) == null ? Level.WARN : Level.DEBUG;
    }
}
