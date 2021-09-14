/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.import_index;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

/**
 * Represents a request to import a particular dangling index.
 */
public class ImportDanglingIndexAction extends ActionType<AcknowledgedResponse> {

    public static final ImportDanglingIndexAction INSTANCE = new ImportDanglingIndexAction();
    public static final String NAME = "cluster:admin/indices/dangling/import";

    private ImportDanglingIndexAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }
}
