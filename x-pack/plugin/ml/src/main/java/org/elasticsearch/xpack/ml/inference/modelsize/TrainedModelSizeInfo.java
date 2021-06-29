/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

interface TrainedModelSizeInfo extends Accountable, NamedXContentObject {
}
