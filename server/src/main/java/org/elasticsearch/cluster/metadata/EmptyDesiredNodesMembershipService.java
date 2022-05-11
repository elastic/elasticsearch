/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

public class EmptyDesiredNodesMembershipService implements DesiredNodesMembershipService {

    public static final DesiredNodesMembershipService INSTANCE = new EmptyDesiredNodesMembershipService();

    private EmptyDesiredNodesMembershipService() {}

    @Override
    public DesiredNodes.MembershipInformation getMembershipInformation() {
        return DesiredNodes.MembershipInformation.EMPTY;
    }
}
