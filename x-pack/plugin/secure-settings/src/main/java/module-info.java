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

import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;
import org.elasticsearch.xpack.stateless.settings.secure.ReservedSecureSettingsHandlerProvider;

module org.elasticsearch.settings.secure {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcore;

    provides ReservedStateHandlerProvider with ReservedSecureSettingsHandlerProvider;
}
