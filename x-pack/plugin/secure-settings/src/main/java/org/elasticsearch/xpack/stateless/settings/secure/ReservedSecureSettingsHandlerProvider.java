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

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

import java.util.Collection;
import java.util.List;

public class ReservedSecureSettingsHandlerProvider implements ReservedStateHandlerProvider {
    @Override
    public Collection<ReservedClusterStateHandler<?>> clusterHandlers() {
        return List.of(new ReservedClusterSecretsAction());
    }

    @Override
    public Collection<ReservedProjectStateHandler<?>> projectHandlers() {
        return List.of(new ReservedProjectSecretsAction());
    }
}
