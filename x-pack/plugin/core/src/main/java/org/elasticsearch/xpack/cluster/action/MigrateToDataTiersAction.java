/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.action.ActionType;

public class MigrateToDataTiersAction extends ActionType<MigrateToDataTiersResponse> {

    public static final MigrateToDataTiersAction INSTANCE = new MigrateToDataTiersAction();
    public static final String NAME = "cluster:admin/migrate_to_data_tiers";

    private MigrateToDataTiersAction() {
        super(NAME, MigrateToDataTiersResponse::new);
    }

}
