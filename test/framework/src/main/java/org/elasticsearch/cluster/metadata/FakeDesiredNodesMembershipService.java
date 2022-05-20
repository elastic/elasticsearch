/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import java.util.List;
import java.util.Set;

public class FakeDesiredNodesMembershipService implements DesiredNodesMembershipService {

    public static final DesiredNodesMembershipService INSTANCE = new FakeDesiredNodesMembershipService();

    private FakeDesiredNodesMembershipService() {}

    @Override
    public DesiredNodes.MembershipInformation getMembershipInformation() {
        return new DesiredNodes.MembershipInformation(null, Set.of()) {
            @Override
            public boolean isMember(DesiredNode desiredNode) {
                throw new UnsupportedOperationException("Unexpected call");
            }

            @Override
            public int memberCount() {
                throw new UnsupportedOperationException("Unexpected call");
            }

            @Override
            public List<DesiredNode> allDesiredNodes() {
                throw new UnsupportedOperationException("Unexpected call");
            }

            @Override
            public Set<DesiredNode> members() {
                throw new UnsupportedOperationException("Unexpected call");
            }
        };
    }
}
