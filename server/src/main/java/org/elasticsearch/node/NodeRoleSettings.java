/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.List;
import java.util.stream.Collectors;

public class NodeRoleSettings {

    public static final Setting<List<DiscoveryNodeRole>> NODE_ROLES_SETTING = Setting.listSetting(
        "node.roles",
        null,
        DiscoveryNode::getRoleFromRoleName,
        settings -> DiscoveryNode.getPossibleRoles()
            .stream()
            .filter(role -> role.isEnabledByDefault(settings))
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toList()),
        roles -> {},
        Property.NodeScope
    );

}
