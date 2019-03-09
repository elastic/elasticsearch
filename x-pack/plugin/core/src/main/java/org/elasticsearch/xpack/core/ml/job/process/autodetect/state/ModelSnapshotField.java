/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.ParseField;

public final class ModelSnapshotField {

    public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");

    private ModelSnapshotField() {}
}
